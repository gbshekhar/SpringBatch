package com.springbatch.config;

import com.springbatch.listener.MyStepExecutionListener;
import com.springbatch.decider.MyJobExecutionDecider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class SpringBatchConfig {

    //Sequential Flow Job
    //@Bean
//    public Job firstSequenceFlowJob(JobRepository jobRepository, PlatformTransactionManager transactionManager, Step firstStep, Step secondStep, Step thirdStep){
//        return new JobBuilder("firstSequenceFlowJob", jobRepository)
//                .preventRestart()
//                .start(firstStep)
//                .next(secondStep)
//                .next(thirdStep)
//                .build();
//    }

    //Conditional Flow Job
    @Bean
    public Job firstConditionalFlowJob(JobRepository jobRepository, PlatformTransactionManager transactionManager, Step firstStep,
                        Step secondStep, Step thirdStep){
        return new JobBuilder("firstConditionalFlowJob", jobRepository)
                .start(firstStep(jobRepository, transactionManager))
                .on("COMPLETED").to(secondStep(jobRepository, transactionManager))
                .from(secondStep(jobRepository, transactionManager)).on("TEST_STATUS").to(thirdStep(jobRepository, transactionManager))
                .from(secondStep(jobRepository, transactionManager)).on("*").to(fourthStep(jobRepository, transactionManager))
                .end()
                .build();
    }

    @Bean
    public Job secondConditionalFlowJob(JobRepository jobRepository, PlatformTransactionManager transactionManager, Step firstStep,
                                       Step secondStep, Step thirdStep){
        return new JobBuilder("secondConditionalFlowJob", jobRepository)
                .start(firstStep(jobRepository, transactionManager))
                .on("COMPLETED").to(myJobExecutionDecider())
                .on("TEST_STATUS").to(secondStep(jobRepository, transactionManager))
                .from(myJobExecutionDecider()).on("*").to(thirdStep(jobRepository, transactionManager))
                .end()
                .build();
    }

    @Bean
    public StepExecutionListener myStepExecutionListener(){
        return new MyStepExecutionListener();
    }

    @Bean
    public JobExecutionDecider myJobExecutionDecider(){
        return new MyJobExecutionDecider();
    }

    @Bean
    @Qualifier("firstStep")
    public Step firstStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("Step1", jobRepository).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
                System.out.println("Step1 Executed");
                return RepeatStatus.FINISHED;
            }
        }, transactionManager).build();
    }

    @Bean
    @Qualifier("secondStep")
    public Step secondStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("Step2", jobRepository).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Step2 Executed");
                boolean throwException = false;
                if(throwException){
                    throw new Exception("Raise Exception");
                }
                return RepeatStatus.FINISHED;
            }
        }, transactionManager)
                .listener(myStepExecutionListener())
                .build();
    }

    @Bean
    @Qualifier("thirdStep")
    public Step thirdStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("Step3", jobRepository).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
                System.out.println("Step3 Executed");
                return RepeatStatus.FINISHED;
            }
        }, transactionManager).build();
    }

    @Bean
    @Qualifier("fourthStep")
    public Step fourthStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("Step4", jobRepository).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
                System.out.println("Step4 Executed");
                return RepeatStatus.FINISHED;
            }
        }, transactionManager).build();
    }
}
