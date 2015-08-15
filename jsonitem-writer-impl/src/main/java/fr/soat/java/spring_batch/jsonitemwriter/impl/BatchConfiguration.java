package fr.soat.java.spring_batch.jsonitemwriter.impl;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import fr.soat.java.spring_batch.jsonitemwriter.api.JsonFlatFileItemWriter;
import fr.soat.java.spring_batch.jsonitemwriter.api.JsonItemAggregator;
import fr.soat.java.spring_batch.jsonitemwriter.model.Person;
import fr.soat.java.spring_batch.jsonitemwriter.processor.PersonItemProcessor;
import fr.soat.java.spring_batch.jsonitemwriter.utils.AppUtils;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration{
	
	private static final String TARGET_SAMPLE_OUTPUT_DATA_JSON = "target/sample-output-data.json";
	private static final String JSON_ROOT_NODE = "Persons";
	
    @Bean
    public ItemReader<Person> reader() {    	
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource(AppUtils.INPUT_FILE));
        
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
		return reader;    	
    }
    
    @Bean
    public ItemProcessor<Person, Person> processor() {
        return new PersonItemProcessor();
    }
    
    @Bean
    public ItemWriter<Person> writer() {
    	//JSON_ROOT_NODE = persons but can be null and in that case just call the default constructor
        JsonFlatFileItemWriter<Person> writer = new JsonFlatFileItemWriter<Person>(JSON_ROOT_NODE);
        
        writer.setJsonItemAggregator(new JsonItemAggregator<Person>());             
        writer.setResource(new FileSystemResource(System.getProperty("user.dir") + File.separator  + TARGET_SAMPLE_OUTPUT_DATA_JSON));
        writer.setEncoding(AppUtils.UTF_8.name());
        writer.setShouldDeleteIfExists(true);

        return writer;
    }
    
    @Bean
    public Job writeJsonFormatJob(JobBuilderFactory jobs, Step stepOne, JobExecutionListener listener) {
        return jobs.get("JsonWriter")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(stepOne)
                .end()
                .build();
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
            ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {
    	
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(2) //commit-interval = 2
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }


}
