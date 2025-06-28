package com.springbatch.config;

import com.springbatch.domain.*;
import com.springbatch.processor.FilterProductItemProcessor;
import com.springbatch.processor.TransformMyOSProductItemProcessor;
import com.springbatch.processor.TransformMyProductItemProcessor;
import com.springbatch.reader.ProductNameItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
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

    @Bean
    public ItemWriter<Product> flatFileItemWriter(){
        FlatFileItemWriter<Product> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource("output/Product_Details_Output.csv"));

        DelimitedLineAggregator<Product> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");

        BeanWrapperFieldExtractor<Product> fieldExtractor= new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"productId", "productName", "productCategory", "productPrice"});

        lineAggregator.setFieldExtractor(fieldExtractor);

        itemWriter.setLineAggregator(lineAggregator);
        return itemWriter;
    }

    @Bean
    public JdbcBatchItemWriter<Product> jdbcBatchItemWriter(){
        JdbcBatchItemWriter<Product> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setDataSource(dataSource);
        jdbcBatchItemWriter.setSql("insert into product_details_output values (:productId, :productName, :productCategory, :productPrice)");
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return jdbcBatchItemWriter;
    }

    @Bean
    public JdbcBatchItemWriter<OSProduct> jdbcBatchItemWriter_osproduct(){
        JdbcBatchItemWriter<OSProduct> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setDataSource(dataSource);
        jdbcBatchItemWriter.setSql("insert into OS_PRODUCT_DETAILS values (:productId, :productName, :productCategory, :productPrice, :taxPercent, :sku, :shippingRate)");
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return jdbcBatchItemWriter;
    }

    @Bean
    public ItemProcessor<Product, Product> transformMyProductItemProcessor(){
        return new TransformMyProductItemProcessor();
    }

    @Bean
    public ItemProcessor<Product, OSProduct> transformMyOSProductItemProcessor(){
        return new TransformMyOSProductItemProcessor();
    }

    @Bean
    public ItemProcessor<Product, Product> filterDataItemProcessor(){
        return new FilterProductItemProcessor();
    }

    @Bean
    public ValidatingItemProcessor<Product> validateItemProcessor(){
        ValidatingItemProcessor<Product> validatingItemProcessor = new ValidatingItemProcessor<>(new ProductValidator());
        validatingItemProcessor.setFilter(true);//If Validation fails then it will not fail Job only filters the record
        return validatingItemProcessor;
    }

    @Bean
    public BeanValidatingItemProcessor<Product> validateBeanItemProcessor(){
        BeanValidatingItemProcessor<Product> beanValidatingItemProcessor = new BeanValidatingItemProcessor<>();
        beanValidatingItemProcessor.setFilter(true);//If Validation fails then it will not fail Job only filters the record
        return  beanValidatingItemProcessor;
    }

    @Bean
    public CompositeItemProcessor<Product, OSProduct> compositeItemProcessor(){
        CompositeItemProcessor<Product, OSProduct> compositeItemProcessor = new CompositeItemProcessor<>();

        List itemProcessorList = new ArrayList<>();
        //itemProcessorList.add(validateItemProcessor());
        itemProcessorList.add(filterDataItemProcessor());
        itemProcessorList.add(transformMyOSProductItemProcessor());

        compositeItemProcessor.setDelegates(itemProcessorList);
        return compositeItemProcessor;
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

//    @Bean
//    @Qualifier("firstStep")
//    public Step firstStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
//        return new StepBuilder("Step1", jobRepository)
//                .<Product, Product>chunk(2, transactionManager)
//                .reader(jdbcCursorItemReader())
//                .writer(new ItemWriter<Product>() {
//                    @Override
//                    public void write(Chunk<? extends Product> chunk) throws Exception {
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
                .<Product, OSProduct>chunk(2, transactionManager)
                .reader(jdbcCursorItemReader())
                .processor(compositeItemProcessor())
                .writer(jdbcBatchItemWriter_osproduct())
                .build();
    }
}
