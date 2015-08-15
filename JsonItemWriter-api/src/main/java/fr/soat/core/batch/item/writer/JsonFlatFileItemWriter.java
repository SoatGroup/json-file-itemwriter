package fr.soat.core.batch.item.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.WriterNotOpenException;
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.util.FileUtils;
import org.springframework.batch.support.transaction.TransactionAwareBufferedWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * 
 * @author Michelle AVOMO
 *
 * @param <T>
 */
public class JsonFlatFileItemWriter<T> extends AbstractItemStreamItemWriter<T>
		implements ResourceAwareItemWriterItemStream<T>, InitializingBean {

	protected static final Log logger = LogFactory
			.getLog(JsonFlatFileItemWriter.class);

	private static final String DEFAULT_LINE_SEPARATOR = System
			.getProperty("line.separator");
	private static final String WRITTEN_STATISTICS_NAME = "written";
	private static final String RESTART_DATA_NAME = "current.count";

	private String encoding = OutputState.DEFAULT_CHARSET;

	private OutputState state;
	private Resource resource;
	private boolean append = false;
	private boolean forceSync = false;
	private boolean saveState = true;
	private boolean shouldDeleteIfExists = true;
	private boolean shouldDeleteIfEmpty = false;
	private boolean transactional = true;

	private String lineSeparator;

	// Our line aggreggator
	private JsonLineAggregator<T> lineAggregator;

	private JsonHeaderFooterCallback headerCallback, footerCallback;

	public JsonFlatFileItemWriter() {
		this.setExecutionContextName(ClassUtils
				.getShortName(JsonFlatFileItemWriter.class));
	}

	/**
	 * Logique d'écriture revue pour mieux gérer l'ajout ou non du séparateur
	 * surtout pour la fin de fichier.
	 */
	@Override
	public void write(List<? extends T> items) throws Exception {
		if (!getOutputState().isInitialized()) {
			throw new WriterNotOpenException(
					"Writer must be open before it can be written to");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Writing to flat file with " + items.size()
					+ " items.");
		}

		OutputState state = getOutputState();
		StringBuilder lines = new StringBuilder();
		int lineCount = 0;
		Iterator<? extends T> it = items.iterator();
		while (it.hasNext()) {
			// Au redémarrage du batch - chunk suivant, ne rajouter une virgule
			// que pour le premier item du chunk
			if (it.hasNext() && lineCount == 0 && state.linesWritten > 0) {
				lines.append(lineSeparator);
			}

			T item = it.next();
			lines.append(lineAggregator.aggregate(item));
			// Ne rajouter la virgule de fin que si on est en fin de fichier.
			if (it.hasNext()) {
				lines.append(lineSeparator);
			}
			lineCount++;
		}
		try {
			state.write(lines.toString());
		} catch (IOException e) {
			throw new WriteFailedException(
					"Could not write data.  The file may be corrupt.", e);
		}
		state.linesWritten += lineCount;

	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(lineAggregator, "A LineAggregator must be provided.");
		if (append) {
			shouldDeleteIfExists = false;
		}
	}

	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public JsonHeaderFooterCallback getFooterCallback() {
		return footerCallback;
	}

	public void setFooterCallback(JsonHeaderFooterCallback footerCallback) {
		this.footerCallback = footerCallback;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}

	public JsonHeaderFooterCallback getHeaderCallback() {
		return headerCallback;
	}

	public void setHeaderCallback(JsonHeaderFooterCallback headerCallback) {
		this.headerCallback = headerCallback;
	}

	public Resource getResource() {
		return resource;
	}

	public void setLineAggregator(JsonLineAggregator<T> lineAggregator) {
		this.lineAggregator = lineAggregator;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public void setShouldDeleteIfExists(boolean shouldDeleteIfExists) {
		this.shouldDeleteIfExists = shouldDeleteIfExists;
	}

	/**
	 * Initialize the reader. This method may be called multiple times before
	 * close is called.
	 * 
	 * @see ItemStream#open(ExecutionContext)
	 */
	@Override
	public void open(ExecutionContext executionContext)
			throws ItemStreamException {
		Assert.notNull(resource, "The resource must be set");
		if (!getOutputState().isInitialized()) {
			this.doOpen(executionContext);
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		if (state == null) {
			throw new ItemStreamException(
					"ItemStream not open or already closed.");
		}
		Assert.notNull(executionContext, "ExecutionContext must not be null");
		if (saveState) {
			try {
				executionContext.putLong(
						getExecutionContextKey(RESTART_DATA_NAME),
						state.position());
			} catch (IOException e) {
				throw new ItemStreamException(
						"ItemStream does not return current position properly",
						e);
			}
			executionContext.putLong(
					getExecutionContextKey(WRITTEN_STATISTICS_NAME),
					state.linesWritten);
		}
	}

	private void doOpen(ExecutionContext executionContext)
			throws ItemStreamException {
		OutputState outputState = getOutputState();
		if (executionContext
				.containsKey(getExecutionContextKey(RESTART_DATA_NAME))) {
			outputState.restoreFrom(executionContext);
		}
		try {
			outputState.initializeBufferedWriter();
		} catch (IOException ioe) {
			throw new ItemStreamException("Failed to initialize writer", ioe);
		}
		if (outputState.lastMarkedByteOffsetPosition == 0
				&& !outputState.appending) {
			if (headerCallback != null) {
				try {
					headerCallback
							.writeHeader(outputState.outputBufferedWriter);
					// Ne pas mettre de virgule après le header du JSON.
					outputState.write(DEFAULT_LINE_SEPARATOR);
				} catch (IOException e) {
					throw new ItemStreamException(
							"Could not write headers.  The file may be corrupt.",
							e);
				}
			}
		}
	}

	/**
	 * @see ItemStream#close()
	 */
	@Override
	public void close() {
		if (state != null) {
			try {
				if (footerCallback != null
						&& state.outputBufferedWriter != null) {

					footerCallback.writeFooter(state.outputBufferedWriter);
					state.outputBufferedWriter.flush();
				}
			} catch (IOException e) {
				throw new ItemStreamException(
						"Failed to write footer before closing", e);
			} finally {
				state.close();
				if (state.linesWritten == 0 && shouldDeleteIfEmpty) {
					try {
						resource.getFile().delete();
					} catch (IOException e) {
						throw new ItemStreamException(
								"Failed to delete empty file on close", e);
					}
				}
				state = null;
			}
		}
	}

	private OutputState getOutputState() {
		if (state == null) {
			File file;
			try {
				file = resource.getFile();
			} catch (IOException e) {
				throw new ItemStreamException(
						"Could not convert resource to file: [" + resource
								+ "]", e);
			}
			Assert.state(!file.exists() || file.canWrite(),
					"Resource is not writable: [" + resource + "]");
			state = new OutputState();
			state.setDeleteIfExists(shouldDeleteIfExists);
			state.setAppendAllowed(append);
			state.setEncoding(encoding);
		}
		return state;
	}

	/**
	 * Encapsulates the runtime state of the writer. All state changing
	 * operations on the writer go through this class.
	 */
	private class OutputState {
		// default encoding for writing to output files - set to UTF-8.
		private static final String DEFAULT_CHARSET = "UTF-8";

		private FileOutputStream os;

		// The bufferedWriter over the file channel that is actually written
		Writer outputBufferedWriter;

		FileChannel fileChannel;

		// this represents the charset encoding (if any is needed) for the
		// output file
		String encoding = DEFAULT_CHARSET;

		boolean restarted = false;

		long lastMarkedByteOffsetPosition = 0;

		long linesWritten = 0;

		boolean shouldDeleteIfExists = true;

		boolean initialized = false;

		private boolean append = false;

		private boolean appending = false;

		/**
		 * Return the byte offset position of the cursor in the output file as a
		 * long integer.
		 */
		public long position() throws IOException {
			long pos = 0;

			if (fileChannel == null) {
				return 0;
			}

			outputBufferedWriter.flush();
			pos = fileChannel.position();
			if (transactional) {
				pos += ((TransactionAwareBufferedWriter) outputBufferedWriter)
						.getBufferSize();
			}

			return pos;

		}

		/**
		 * @param append
		 */
		public void setAppendAllowed(boolean append) {
			this.append = append;
		}

		/**
		 * @param executionContext
		 */
		public void restoreFrom(ExecutionContext executionContext) {
			lastMarkedByteOffsetPosition = executionContext
					.getLong(getExecutionContextKey(RESTART_DATA_NAME));
			linesWritten = executionContext
					.getLong(getExecutionContextKey(WRITTEN_STATISTICS_NAME));
			if (shouldDeleteIfEmpty && linesWritten == 0) {
				// previous execution deleted the output file because no items
				// were written
				restarted = false;
				lastMarkedByteOffsetPosition = 0;
			} else {
				restarted = true;
			}
		}

		/**
		 * @param shouldDeleteIfExists
		 */
		public void setDeleteIfExists(boolean shouldDeleteIfExists) {
			this.shouldDeleteIfExists = shouldDeleteIfExists;
		}

		/**
		 * @param encoding
		 */
		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}

		/**
		 * Close the open resource and reset counters.
		 */
		public void close() {

			initialized = false;
			restarted = false;
			try {
				if (outputBufferedWriter != null) {
					outputBufferedWriter.close();
				}
			} catch (IOException ioe) {
				throw new ItemStreamException(
						"Unable to close the the ItemWriter", ioe);
			} finally {
				if (!transactional) {
					closeStream();
				}
			}
		}

		private void closeStream() {
			try {
				if (fileChannel != null) {
					fileChannel.close();
				}
			} catch (IOException ioe) {
				throw new ItemStreamException(
						"Unable to close the the ItemWriter", ioe);
			} finally {
				try {
					if (os != null) {
						os.close();
					}
				} catch (IOException ioe) {
					throw new ItemStreamException(
							"Unable to close the the ItemWriter", ioe);
				}
			}
		}

		/**
		 * @param line
		 * @throws IOException
		 */
		public void write(String line) throws IOException {
			if (!initialized) {
				initializeBufferedWriter();
			}

			outputBufferedWriter.write(line);
			outputBufferedWriter.flush();
		}

		/**
		 * Truncate the output at the last known good point.
		 * 
		 * @throws IOException
		 */
		public void truncate() throws IOException {
			fileChannel.truncate(lastMarkedByteOffsetPosition);
			fileChannel.position(lastMarkedByteOffsetPosition);
		}

		/**
		 * Creates the buffered writer for the output file channel based on
		 * configuration information.
		 * 
		 * @throws IOException
		 */
		private void initializeBufferedWriter() throws IOException {

			File file = resource.getFile();
			FileUtils.setUpOutputFile(file, restarted, append,
					shouldDeleteIfExists);

			os = new FileOutputStream(file.getAbsolutePath(), true);
			fileChannel = os.getChannel();

			outputBufferedWriter = getBufferedWriter(fileChannel, encoding);
			outputBufferedWriter.flush();

			if (append) {
				// Bug in IO library? This doesn't work...
				// lastMarkedByteOffsetPosition = fileChannel.position();
				if (file.length() > 0) {
					appending = true;
					// Don't write the headers again
				}
			}

			Assert.state(outputBufferedWriter != null);
			// in case of restarting reset position to last committed point
			if (restarted) {
				checkFileSize();
				truncate();
			}

			initialized = true;
		}

		public boolean isInitialized() {
			return initialized;
		}

		/**
		 * Returns the buffered writer opened to the beginning of the file
		 * specified by the absolute path name contained in absoluteFileName.
		 */
		private Writer getBufferedWriter(FileChannel fileChannel,
				String encoding) {
			try {
				final FileChannel channel = fileChannel;
				if (transactional) {
					TransactionAwareBufferedWriter writer = new TransactionAwareBufferedWriter(
							channel, new Runnable() {
								@Override
								public void run() {
									closeStream();
								}
							});

					writer.setEncoding(encoding);
					writer.setForceSync(forceSync);
					return writer;
				} else {
					Writer writer = new BufferedWriter(Channels.newWriter(
							fileChannel, encoding)) {
						@Override
						public void flush() throws IOException {
							super.flush();
							if (forceSync) {
								channel.force(false);
							}
						}
					};

					return writer;
				}
			} catch (UnsupportedCharsetException ucse) {
				throw new ItemStreamException(
						"Bad encoding configuration for output file "
								+ fileChannel, ucse);
			}
		}

		/**
		 * Checks (on setState) to make sure that the current output file's size
		 * is not smaller than the last saved commit point. If it is, then the
		 * file has been damaged in some way and whole task must be started over
		 * again from the beginning.
		 * 
		 * @throws IOException
		 *             if there is an IO problem
		 */
		private void checkFileSize() throws IOException {
			long size = -1;

			outputBufferedWriter.flush();
			size = fileChannel.size();

			if (size < lastMarkedByteOffsetPosition) {
				throw new ItemStreamException(
						"Current file size is smaller than size at last commit");
			}
		}

	}

}
