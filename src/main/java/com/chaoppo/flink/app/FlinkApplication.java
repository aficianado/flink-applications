package com.chaoppo.flink.app;

import com.chaoppo.flink.app.flinkeventhub.eventhub.FlinkEventHubConsumerClient;
import com.chaoppo.flink.app.flinkeventhub.model.EventData;
import com.chaoppo.flink.app.flinkeventhub.sf.HBaseSourceFunction;
import com.chaoppo.flink.app.job.FlinkBaseJob;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class FlinkApplication {

    public static void main(String[] args) throws Exception {
        ApplicationContext applicationContext = SpringApplication.run(FlinkApplication.class, args);
        FlinkBaseJob flinkBaseJob = applicationContext.getBean(FlinkBaseJob.class);
        flinkBaseJob.executeBaseJob();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        FlinkEventHubConsumerClient flinkEventHubConsumerClient = new FlinkEventHubConsumerClient(
                "Endpoint=sb:CONN-STRING", "$DEFAULT");

        HBaseSourceFunction messageSourceFunction = new HBaseSourceFunction(flinkEventHubConsumerClient,
                "localhost:2181", "8", 4, true);
        DataStream<EventData> stream = env.addSource(messageSourceFunction);
        stream.map((EventData eventData) -> {
            //client side offset is saving in zookeeper
            //messageSourceFunction.saveOffset(eventData.getFullyQualifiedNamespace(), eventData.getEventHubName(), eventData.getPartitionId(), eventData.getOffset().toString());
            System.out.println("First Processor:" + eventData);
            return eventData.getOffset();
        }).map((Long offset) -> {
            //client side offset is saving in zookeeper
            //messageSourceFunction.saveOffset(eventData.getFullyQualifiedNamespace(), eventData.getEventHubName(), eventData.getPartitionId(), eventData.getOffset().toString());
            System.out.println("Second Processor:" + offset);
            //            System.out.println(10 / 0);
            return offset;
        }).print();

        env.execute("MsgSourceFunction");
    }

}
