package com.springbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class MyStepExecutionListener implements StepExecutionListener {

    public void beforeStep(StepExecution stepExecution){
        System.out.println("inside MyStepExecutionListener: beforeStep");
    }

    public ExitStatus afterStep(StepExecution stepExecution){
        System.out.println("inside MyStepExecutionListener: afterStep");
        return new ExitStatus("TEST_STATUS");
    }
}
