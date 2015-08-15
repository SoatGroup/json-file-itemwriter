package fr.soat.java.spring_batch.jsonitemwriter.api;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.transform.LineAggregator;

import com.fasterxml.jackson.core.JsonProcessingException;

import fr.soat.java.spring_batch.jsonitemwriter.api.utils.JsonUtils;

/**
 * 
 * 
 * @author Michelle AVOMO
 *
 * @param <T>
 */
public class JsonItemAggregator<T> implements LineAggregator<T> {
	
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JsonItemAggregator.class);

	@Override
	public String aggregate(T item) {
		String result = null;
		try {
			result = JsonUtils.convertObjectToJsonString(item); 

		} catch (JsonProcessingException jpe) {
			logger.error("An error has occured. Error message {} ", jpe.getMessage() );
		}		
		return result;
	}
}
