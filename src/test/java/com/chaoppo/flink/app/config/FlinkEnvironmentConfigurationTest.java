package com.chaoppo.flink.app.config;

import com.chaoppo.flink.app.FlinkApplication;
import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = FlinkApplication.class)
class FlinkEnvironmentConfigurationTest {

    @Autowired
    @InjectMocks
    FlinkEnvironmentConfiguration flinkEnvironmentConfiguration;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEnvironmentWithWebUI() {
        Assert.assertNotNull(flinkEnvironmentConfiguration.createEnvironmentWithWebUI());
    }

    @Test
    public void testCreateEnvironment() {
        Assert.assertNotNull(flinkEnvironmentConfiguration.createEnvironment());
    }

}
