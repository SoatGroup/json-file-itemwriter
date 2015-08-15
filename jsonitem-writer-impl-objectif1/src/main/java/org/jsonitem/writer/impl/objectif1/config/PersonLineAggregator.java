package org.jsonitem.writer.impl.objectif1.config;

import org.springframework.batch.item.file.transform.LineAggregator;

import fr.soat.java.spring_batch.jsonitemwriter.utils.AppUtils;

public class PersonLineAggregator<Person> implements LineAggregator<Person> {

	@Override
	public String aggregate(Person person) {
		return person.toString() + AppUtils.LINE_SEPARATOR;
	}
}
