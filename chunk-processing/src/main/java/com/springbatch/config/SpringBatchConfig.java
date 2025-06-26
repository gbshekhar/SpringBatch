package com.springbatch.config;

import com.springbatch.domain.Product;
import com.springbatch.domain.ProductFieldSetMapper;
import com.springbatch.domain.ProductRowMapper;
import com.springbatch.reader.ProductNameItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.List;

@Configuration
public class SpringBatchConfig {

    @Autowired
    DataSource dataSource;

    @Bean
    public ItemReader<String> itemReader(){
        List<String> productList = List.of("Product1", "Product2", "Product3", "Product4");
        return new ProductNameItemReader(productList);
    }

    @Bean
    public ItemReader<Product> flatFileItemReader(){
        FlatFileItemReader<Product> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setResource(new ClassPathResource("/data/Product_Details.csv"));

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("product_id", "product_name", "product_category", "product_price");

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(new ProductFieldSetMapper());

        flatFileItemReader.setLineMapper(lineMapper);
        return  flatFileItemReader;
    }

    @Bean
    public ItemReader<Product> jdbcCursorItemReader(){
        JdbcCursorItemReader<Product> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("select * from product_details order by product_id;");
        itemReader.setRowMapper(new ProductRowMapper());
        return itemReader;
    }

    @Bean
    public ItemReader<Product> jdbcPagingItemReader() throws Exception {
        JdbcPagingItemReader<Product> itemReader = new JdbcPagingItemReader<>();
        itemReader.setDataSource(dataSource);

        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setDataSource(dataSource);
        factory.setSelectClause("select product_id, product_name, product_category, product_price");
        factory.setFromClause("from product_details");
        factory.setSortKey("product_id");

        itemReader.setQueryProvider(factory.getObject());
        itemReader.setRowMapper(new ProductRowMapper());
        itemReader.setPageSize(2);
        return itemReader;
    }

    //Sequential Flow Job
    @Bean
    public Job firstJob(JobRepository jobRepository, PlatformTransactionManager transactionManager, Step firstStep){
        return new JobBuilder("firstSequenceFlowJob", jobRepository)
                .preventRestart()
                .start(firstStep)
                .build();
    }

//    @Bean
//    @Qualifier("firstStep")
//    public Step firstStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
//        return new StepBuilder("Step1", jobRepository)
//                .<String, String>chunk(2, transactionManager)
//                .reader(itemReader())
//                .writer(new ItemWriter<String>() {
//                    @Override
//                    public void write(Chunk<? extends String> chunk) throws Exception {
//                        System.out.println("Chunk Processing Started");
//                        chunk.getItems().forEach(System.out::println);
//                        System.out.println("Chunk Processing Ended");
//                    }
//                })
//                .build();
//    }

    @Bean
    @Qualifier("firstStep")
    public Step firstStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("Step1", jobRepository)
                .<Product, Product>chunk(2, transactionManager)
                .reader(jdbcCursorItemReader())
                .writer(new ItemWriter<Product>() {
                    @Override
                    public void write(Chunk<? extends Product> chunk) throws Exception {
                        System.out.println("Chunk Processing Started");
                        chunk.getItems().forEach(System.out::println);
                        System.out.println("Chunk Processing Ended");
                    }
                })
                .build();
    }
}
