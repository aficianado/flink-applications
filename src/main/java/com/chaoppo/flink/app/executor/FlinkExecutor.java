package com.chaoppo.flink.app.executor;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FlinkExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutor.class);

    public JobExecutionResult execute(StreamExecutionEnvironment executionEnvironment, String jobName) {
        JobExecutionResult jobExecutionResult = null;
        try {
            jobExecutionResult = executionEnvironment.execute(jobName);
        } catch (Exception e) {
            LOG.info("Exception while executing the job {}", e);
        }
        return jobExecutionResult;
    }

}
