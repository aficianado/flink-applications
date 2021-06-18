package com.chaoppo.flink.app.flinkeventhub.sf;

import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.chaoppo.flink.app.flinkeventhub.eventhub.FlinkEventHubConsumerClient;
import com.chaoppo.flink.app.flinkeventhub.model.EventData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.storm.eventhubs.spout.ZookeeperStateStore;
import org.springframework.core.task.TaskExecutor;

import java.util.Locale;

public class AsyncMessageSourceFunction implements SourceFunction<EventData> {

    private static final String SEPARATOR = "/";
    private static final long serialVersionUID = 6128016096756071380L;
    FlinkEventHubConsumerClient flinkEventHubConsumerClient;
    String zookeeperHost;
    private volatile boolean isRunning = true;
    private transient ZookeeperStateStore zookeeperStateStore;
    private transient EventHubConsumerAsyncClient eventHubConsumerAsyncClient;

    public AsyncMessageSourceFunction(String zookeeperHost, FlinkEventHubConsumerClient flinkEventHubConsumerClient) {
        this.zookeeperHost = zookeeperHost;
        this.flinkEventHubConsumerClient = flinkEventHubConsumerClient;
    }

    public static void main(String arg[]) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEventHubConsumerClient flinkEventHubConsumerClient = new FlinkEventHubConsumerClient(
                "Endpoint=sb://CONN-STRING", "$DEFAULT");
        new AsyncMessageSourceFunction("localhost:2181", flinkEventHubConsumerClient).receiveAll();

        //        FlinkZookeeperStateStore flinkZookeeperStateStore = new FlinkZookeeperStateStore("localhost:2181");
        //        DataStream<EventData> stream = env.addSource(new AsyncMessageSourceFunction("localhost:2181", flinkEventHubConsumerClient));
        //        stream.print();
        //        env.execute("MsgSourceFunction");
    }

    public void receiveAll() {
        zookeeperStateStore = new ZookeeperStateStore(zookeeperHost);
        eventHubConsumerAsyncClient = flinkEventHubConsumerClient.createEventHubConsumerAsyncClient();

        eventHubConsumerAsyncClient.receive(true).subscribe(partitionEvent -> {
            EventData eventData = getEventData(partitionEvent);

            String partitionStatePath = preparePartitionStatePath(partitionEvent);
            zookeeperStateStore.saveData(partitionStatePath, partitionEvent.getData().getSequenceNumber().toString());

            String message = zookeeperStateStore.readData(partitionStatePath);
            System.out.println(message);

            System.out.printf("Event %s is from partition %s%n.", eventData.getSequenceNumber(),
                    eventData.getPartitionId());
        });
    }

    private EventData getEventData(PartitionEvent partitionEvent) {
        EventData eventData = new EventData(true);
        eventData.setData(partitionEvent.getData().getBodyAsString().getBytes());
        eventData.setPartitionId(String.valueOf(partitionEvent.getData().getSequenceNumber()));
        eventData.setEventHubName(partitionEvent.getPartitionContext().getEventHubName());
        eventData.setOffset(partitionEvent.getData().getSequenceNumber());
        eventData.setConsumerGroup(partitionEvent.getPartitionContext().getConsumerGroup());
        eventData.setEnqueuedTime(partitionEvent.getData().getEnqueuedTime());
        eventData.setPartitionKey(partitionEvent.getData().getPartitionKey());
        eventData.setAnnotation("offset", partitionEvent.getData().getSequenceNumber());
        eventData.setAnnotation("partition", partitionEvent.getPartitionContext().getPartitionId());
        eventData.setAnnotation("topic", partitionEvent.getPartitionContext().getEventHubName());
        return eventData;
    }

    private static String preparePartitionStatePath(PartitionEvent partitionEvent) {
        // Partition state path =
        // "/{prefix}/{topologyName}/{namespace}/{entityPath}/partitions/{partitionId}/state";
        return new StringBuilder().append(SEPARATOR)
                .append(partitionEvent.getPartitionContext().getFullyQualifiedNamespace()).append(SEPARATOR)
                .append(partitionEvent.getPartitionContext().getEventHubName()).append(SEPARATOR)
                .append(partitionEvent.getData().getSequenceNumber()).append(SEPARATOR)
                .append(partitionEvent.getPartitionContext().getPartitionId()).append(SEPARATOR).append("state")
                .toString().toLowerCase(Locale.ROOT);
    }

    @Override
    public void run(SourceContext<EventData> sourceContext) throws Exception {

        zookeeperStateStore = new ZookeeperStateStore(zookeeperHost);
        eventHubConsumerAsyncClient = flinkEventHubConsumerClient.createEventHubConsumerAsyncClient();
        zookeeperStateStore.open();
        new TaskExecutor() {

            @Override
            public void execute(Runnable runnable) {
                eventHubConsumerAsyncClient.receive(true).subscribe(partitionEvent -> {
                    EventData eventData = getEventData(partitionEvent);

                    String partitionStatePath = preparePartitionStatePath(partitionEvent);
                    zookeeperStateStore
                            .saveData(partitionStatePath, partitionEvent.getData().getSequenceNumber().toString());

                    String message = zookeeperStateStore.readData(partitionStatePath);
                    System.out.println(message);
                    if (isRunning) {
                        synchronized (sourceContext.getCheckpointLock()) {
                            sourceContext.collect(eventData);
                        }
                    }
                    System.out.printf("Event %s is from partition %s%n.", eventData.getSequenceNumber(),
                            eventData.getPartitionId());
                });
                try {
                    Thread.sleep(999999999);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

    }

    @Override
    public void cancel() {
        isRunning = false;
        zookeeperStateStore.close();
    }

    @Override
    public String toString() {
        return zookeeperStateStore.toString();
    }
}