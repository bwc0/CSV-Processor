package com.csvreader.config;

import com.csvreader.model.Employee;
import com.csvreader.utils.ConsoleItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private JobBuilderFactory jobs;
    private StepBuilderFactory steps;

    @Value("files/*.csv")
    private Resource[] inputResources;

    public BatchConfig(JobBuilderFactory jobs, StepBuilderFactory steps) {
        this.jobs = jobs;
        this.steps = steps;
    }

    @Bean
    public Job readCsvFileJob() {
        return jobs.get("readCsvFileJob")
                .incrementer(new RunIdIncrementer())
                .start(step())
                .build();
    }

    @Bean
    public Step step() {
        return steps.get("step").<Employee, Employee>chunk(5)
                .reader(multiResourceItemReader())
                .writer(writer())
                .build();
    }

    @Bean
    public MultiResourceItemReader<Employee> multiResourceItemReader() {
        MultiResourceItemReader<Employee> resourceItemReader = new MultiResourceItemReader<>();
        resourceItemReader.setResources(inputResources);
        resourceItemReader.setDelegate(reader());
        return resourceItemReader;
    }

    @Bean
    public FlatFileItemReader<Employee> reader() {
        //Create reader instance
        FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();

        //Set number of lines to skip (skip if it has headers)
        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to values
        reader.setLineMapper(new DefaultLineMapper<Employee>() {
            {
                // 3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("id", "firstName", "lastName");
                    }
                });
                // Set values in Employee class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {
                    {
                        setTargetType(Employee.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    public ConsoleItemWriter<Employee> writer() {
        return new ConsoleItemWriter<>();
    }
}
