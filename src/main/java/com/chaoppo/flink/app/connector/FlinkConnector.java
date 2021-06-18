package com.chaoppo.flink.app.connector;

import com.chaoppo.flink.app.api.Msg;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class FlinkConnector {

    public FlinkKafkaConsumer configKafkaConsumer(String topicName, Properties properties) {

        return new FlinkKafkaConsumer(topicName, new Msg(true), properties);
    }
}
