package org.jsonitem.writer.impl.objectif1.config;

import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;

import fr.soat.java.spring_batch.jsonitemwriter.utils.AppUtils;

public class PersonHeaderFooterCallBack implements FlatFileHeaderCallback, FlatFileFooterCallback{
	
	private static final String OUTPUT_HEADER = "#Persons";
	private static final String OUTPUT_FOOTER = "#eof";
	
	@Override
	public void writeHeader(Writer writer) throws IOException {
		writer.write(OUTPUT_HEADER + AppUtils.LINE_SEPARATOR);		
	}

	@Override
	public void writeFooter(Writer writer) throws IOException {
		writer.write(OUTPUT_FOOTER);
	}
}
