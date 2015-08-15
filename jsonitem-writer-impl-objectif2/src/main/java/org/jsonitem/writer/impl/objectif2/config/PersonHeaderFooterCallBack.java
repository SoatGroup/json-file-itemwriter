package org.jsonitem.writer.impl.objectif2.config;

import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;

import com.google.gson.stream.JsonWriter;

public class PersonHeaderFooterCallBack implements FlatFileHeaderCallback, FlatFileFooterCallback{
	
	private static final String JSON_ROOT_NODE = "Persons";
	private JsonWriter jsonWriter;
	
	@Override
	public void writeHeader(Writer writer) throws IOException {
		this.jsonWriter = new JsonWriter(writer);
		jsonWriter.beginObject().name(JSON_ROOT_NODE).beginArray();		
	}

	@Override
	public void writeFooter(Writer writer) throws IOException {
		jsonWriter.endArray().endObject();
		jsonWriter.close();
	}
}
