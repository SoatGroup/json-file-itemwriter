package fr.soat.core.batch.item.writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.transform.LineAggregator;

import com.fasterxml.jackson.core.JsonProcessingException;

import fr.soat.core.batch.item.writer.utils.JsonUtils;

/**
 * 
 * 
 * @author Michelle AVOMO
 *
 * @param <T>
 */
public class JsonLineAggregator<T> implements LineAggregator<T> {
	
	protected static final Log logger = LogFactory.getLog(JsonLineAggregator.class);

	@Override
	public String aggregate(T item) {
		String result = null;
		try {
			result = JsonUtils.convertObjectToJsonString(item); 

		} catch (JsonProcessingException jpe) {
			logger.warn("An error has occured " + jpe.getMessage() );
		}
		
		return result;
	}
	
	
}
