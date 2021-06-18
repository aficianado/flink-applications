package com.chaoppo.flink.app.config;

import com.chaoppo.flink.app.common.Constants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkEnvironmentConfiguration {

    @Value("${web.ui.port}")
    private String webUiPort;

    public StreamExecutionEnvironment createEnvironmentWithWebUI() {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.setBoolean(Constants.START_LOCAL_FLINK_WEB_UI, true);
        config.setInteger(Constants.WEB_UI_PORT, Integer.parseInt(webUiPort));
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
    }

    public StreamExecutionEnvironment createEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

}
