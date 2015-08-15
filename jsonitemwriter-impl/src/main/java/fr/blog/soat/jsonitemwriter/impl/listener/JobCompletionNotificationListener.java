package fr.blog.soat.jsonitemwriter.impl.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
	
    private static final Log logger = LogFactory.getLog(JobCompletionNotificationListener.class);
    
    @Override
	public void afterJob(JobExecution jobExecution) {
    	if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
    		logger.info("!!! JOB FINISHED! Time to verify the results");
    	}
    }


}
