package com.chaoppo.flink.app.connector;

import com.chaoppo.flink.app.FlinkApplication;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = FlinkApplication.class)
public class FlinkConnectorTest {

    @Autowired
    @InjectMocks
    FlinkConnector flinkConnector;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConfigKafkaConsumer() {
        Properties properties = new Properties();
        Assert.assertNotNull(flinkConnector.configKafkaConsumer("ddec", properties));
    }
}
