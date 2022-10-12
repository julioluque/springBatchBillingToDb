package com.example.demo.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import com.example.demo.model.Student;

@Configuration
@EnableBatchProcessing
public class StudentBatchConfiguration {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private DataSource dataSource;

	public FlatFileItemReader<Student> readFromCsv() {
		FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
		reader.setResource(new FileSystemResource("C://Users/Julio/wrumboMD/csv_input.csv"));
		// reader.setResource(new ClassPathResource("csv_input.csv"));
//		reader.setStrict(false);
		reader.setLineMapper(new DefaultLineMapper<Student>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(Student.fields());
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {
					{
						setTargetType(Student.class);
					}
				});
			}
		});

		return reader;
	}

	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB() {
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(dataSource);
		writer.setSql(
				"insert into csvtodbdata (id, firstName, lastName, email) values (:id, :firstName, :lastName, :email)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<Student, Student>chunk(10).reader(readFromCsv()).writer(writerIntoDB())
				.build();
	}

	@Bean
	public Job job() {
		return jobBuilderFactory.get("job").flow(step()).end().build();
	}

}
