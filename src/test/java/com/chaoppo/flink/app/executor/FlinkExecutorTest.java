package com.chaoppo.flink.app.executor;

import com.chaoppo.flink.app.FlinkApplication;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = FlinkApplication.class)
public class FlinkExecutorTest {

    @Autowired
    @InjectMocks
    FlinkExecutor flinkExecutor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConfigKafkaConsumer() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        Assert.assertNull(flinkExecutor.execute(streamExecutionEnvironment, "test run"));
    }

}
