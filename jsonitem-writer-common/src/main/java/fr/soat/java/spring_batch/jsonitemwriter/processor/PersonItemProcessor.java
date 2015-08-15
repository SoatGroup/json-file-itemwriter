package fr.soat.java.spring_batch.jsonitemwriter.processor;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import fr.soat.java.spring_batch.jsonitemwriter.model.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {
	
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PersonItemProcessor.class);


	@Override
	public Person process(Person person) throws Exception {
		final String firstName = person.getFirstname().toUpperCase();
		final String lastName = person.getLastname().toUpperCase();

		final Person transformedPerson = new Person(firstName, lastName);

		 if (logger.isInfoEnabled()){
	    	 logger.info(">Successfully proceed person from {} to {}", person, transformedPerson);
	     }

		return transformedPerson;
	}

}
