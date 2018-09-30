package com.csvreader.config;

import com.csvreader.model.Employee;
import com.csvreader.processor.ValidationProcessor;
import com.csvreader.utils.ConsoleItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
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
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public ItemProcessor<Employee, Employee> processor() {
        return new ValidationProcessor();
    }

    @Bean
    public FlatFileItemReader<Employee> reader() {
        //Create reader instance
        FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();
        reader.setLineMapper(lineMapper());
        reader.setLinesToSkip(1);
        reader.setResource(new FileSystemResource("files/data.csv"));
        return reader;
    }

    @Bean
    public LineMapper<Employee> lineMapper() {
        DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("id", "firstName", "lastName");
        lineTokenizer.setIncludedFields(0, 1, 2);
        BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Employee.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public ConsoleItemWriter<Employee> writer() {
        return new ConsoleItemWriter<>();
    }
}
