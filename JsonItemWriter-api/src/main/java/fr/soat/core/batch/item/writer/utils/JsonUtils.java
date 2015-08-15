package fr.soat.core.batch.item.writer.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * 
 * @author Michelle AVOMO
 *
 */
public class JsonUtils {

	public static <T> String convertObjectToJsonString(T object) throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();
		return objectWriter.writeValueAsString(object);
	}
}
