package org.jsonitem.writer.impl.objectif2.config;

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
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import fr.soat.java.spring_batch.jsonitemwriter.item.PersonJsonItemAggregator;
import fr.soat.java.spring_batch.jsonitemwriter.model.Person;
import fr.soat.java.spring_batch.jsonitemwriter.processor.PersonItemProcessor;
import fr.soat.java.spring_batch.jsonitemwriter.utils.AppUtils;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	private static final String TARGET_SAMPLE_OUTPUT_DATA_JSON = "target/json-sample-output-data.json";

	
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
        FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
        writer.setLineSeparator(AppUtils.COMMA_SEPARATOR);
        
        //Setting header and footer.
        PersonHeaderFooterCallBack headerFooterCallback = new PersonHeaderFooterCallBack();
        writer.setHeaderCallback(headerFooterCallback);
        writer.setFooterCallback(headerFooterCallback);

        writer.setLineAggregator(new PersonJsonItemAggregator<Person>());       
        
       writer.setResource(new FileSystemResource(System.getProperty("user.dir") + File.separator  + TARGET_SAMPLE_OUTPUT_DATA_JSON));
        writer.setEncoding(AppUtils.UTF_8.name());
        writer.setShouldDeleteIfExists(true);

        return writer;
    }

    @Bean
    public Job writeStepJob(JobBuilderFactory jobs, Step stepOne, JobExecutionListener listener) {
        return jobs.get("writeStepJob")
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
