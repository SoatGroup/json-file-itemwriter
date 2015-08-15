package fr.soat.java.spring_batch.jsonitemwriter.api;

import java.io.IOException;
import java.io.Writer;

import com.google.gson.stream.JsonWriter;

/**
 * 
 * @author Michelle AVOMO
 *
 */
public class JsonHeaderFooterCallback implements JsonFileHeaderFooterCallback {
	
	private JsonWriter jsonWriter;
	private String rootNode;	

	@Override
	public void writeHeader(Writer writer) throws IOException {		
		this.jsonWriter = new JsonWriter(writer);
		if (rootNode != null && !rootNode.isEmpty()){
			jsonWriter.beginObject().name(rootNode).beginArray();
		}else {
			jsonWriter.beginArray();
		}
	}

	@Override
	public void writeFooter(Writer writer) throws IOException {
		if (rootNode != null && !rootNode.isEmpty()){
			jsonWriter.endArray().endObject();
		}else {
			jsonWriter.endArray();
		}
		jsonWriter.close();		
	}
	
	public void setRootNode(String rootNode) {
		this.rootNode = rootNode;
	}
	
	
	
}
