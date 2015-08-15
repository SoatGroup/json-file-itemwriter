package org.jsonitem.writer.impl.objectif1.config;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport  {
	
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JobCompletionNotificationListener.class);
	    
	    @Override
		public void afterJob(JobExecution jobExecution) {
	    	if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
	    		logger.info("Job execution was successful");
	    	}else {
	    		logger.info("Job failed with status {}",jobExecution.getStatus() );
	    	}
	    }

}
