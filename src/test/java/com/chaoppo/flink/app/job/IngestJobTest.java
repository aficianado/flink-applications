package com.chaoppo.flink.app.job;

import com.chaoppo.flink.app.FlinkApplication;
import com.chaoppo.flink.app.connector.FlinkConnector;
import com.chaoppo.flink.app.executor.FlinkExecutor;
import com.daimler.app.job.ingest.IngestJob;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Properties;

@PropertySource("classpath:application-test.properties ")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = FlinkApplication.class)
public class IngestJobTest {

    @Autowired
    @InjectMocks
    IngestJob ingestJob;

    @Autowired
    @InjectMocks
    FlinkConnector flinkConnector;

    @Autowired
    @InjectMocks
    FlinkExecutor flinkExecutor;
    @Value("${ddec.ingest.kafka.topic}")
    private String ddecTestIngestKafkaTopic;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        ingestJob = Mockito.mock(IngestJob.class);
    }

    @Test
    public void testDdecIngestJob() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        Properties properties = new Properties();
        Assert.assertNull(ingestJob.ingestJob(streamExecutionEnvironment,
                flinkConnector.configKafkaConsumer(ddecTestIngestKafkaTopic, properties), flinkExecutor));
    }
}
