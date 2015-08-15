package fr.soat.java.spring_batch.jsonitemwriter.api;

import java.io.IOException;
import java.io.Writer;

/**
 * Callback interface handling both header and footer writing to a file.
 * Both methods write contents to a file using the supplied {@link Writer}. 
 * It is not required to flush the writer inside those methods.
 * 
 * @author Michelle
 */
public interface JsonFileHeaderFooterCallback {
	
	void writeHeader(Writer writer) throws IOException;
	
	void writeFooter(Writer writer) throws IOException;
}
