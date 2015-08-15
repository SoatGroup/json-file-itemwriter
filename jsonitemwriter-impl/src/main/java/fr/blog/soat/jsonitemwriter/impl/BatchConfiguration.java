package fr.blog.soat.jsonitemwriter.impl;

import java.io.File;
import java.nio.charset.Charset;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
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

import fr.blog.soat.jsonitemwriter.impl.model.Person;
import fr.blog.soat.jsonitemwriter.impl.processor.PersonItemProcessor;
import fr.soat.core.batch.item.writer.JsonFlatFileItemWriter;
import fr.soat.core.batch.item.writer.JsonHeaderFooterCallback;
import fr.soat.core.batch.item.writer.JsonLineAggregator;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration  extends DefaultBatchConfigurer{
	
	private static final String JSON_ROOT_NODE = "Persons";
	private static final String LINE_SEPARATOR = ",";
	private static final String INPUT_FILE = "sample-data.csv";
	//private static final String OUTPUT_FILE  = "sample-output-data.json";
    public static final Charset UTF_8 = Charset.forName("UTF-8");

	
	// tag::readerwriterprocessor[]
    @Bean
    public ItemReader<Person> reader() {    	
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource(INPUT_FILE));
        
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
    public ItemWriter<Person> writer(DataSource dataSource) {
        JsonFlatFileItemWriter<Person> writer = new JsonFlatFileItemWriter<Person>();
        
        //Setting header and footer.
        JsonHeaderFooterCallback headerFooterCallBack = new JsonHeaderFooterCallback();
        headerFooterCallBack.setRootNode(JSON_ROOT_NODE);        
        writer.setHeaderCallback(headerFooterCallBack);
        writer.setFooterCallback(headerFooterCallBack);

        writer.setLineSeparator(LINE_SEPARATOR);
        writer.setLineAggregator(new JsonLineAggregator<Person>());       
        
       writer.setResource(new FileSystemResource(System.getProperty("user.dir") + File.separator  + "src/main/resources/sample-output-data.json"));
        writer.setEncoding(UTF_8.name());
       // writer.setAppend(true);
        writer.setShouldDeleteIfExists(true);

        return writer;
    }
    
    // end::readerwriterprocessor[]
    
    // tag::jobstep[]
    @Bean
    public Job writeJsonFormatJob(JobBuilderFactory jobs, Step stepOne, JobExecutionListener listener) {
        return jobs.get("writeJsonFormatJob")
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
