package fr.soat.java.spring_batch.jsonitemwriter.item;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.transform.LineAggregator;

import com.fasterxml.jackson.core.JsonProcessingException;

import fr.soat.java.spring_batch.jsonitemwriter.utils.JsonUtils;

public class PersonJsonItemAggregator<Person> implements LineAggregator<Person> {
	
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PersonJsonItemAggregator.class);


	@Override
	/** Utilise le Mapper et la writer de Jackson pour parser le pojo en chaîne de caractère
	 * 
	 * La méthode <code>JsonUtils.convertObjectToJsonString()</code> est définie telle que : 
	 * 
	 * ObjectMapper objectMapper = new ObjectMapper();
	 * ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();
	 * objectWriter.writeValueAsString(object);
	 * 
	 * {@link org.batchitemwiter.gson.utils.JsonUtils}
	 * 
	 */
	public String aggregate(Person person) {
		String result = null;
		try {
			result = JsonUtils.convertObjectToJsonString(person); 
		} catch (JsonProcessingException jpe) {
			logger.error("An error has occured. Error message {} ", jpe.getMessage() );
		}
		return result;
	}
}
