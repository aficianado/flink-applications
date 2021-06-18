package com.chaoppo.flink.app.flinkeventhub.sf;

import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.chaoppo.flink.app.flinkeventhub.eventhub.FlinkEventHubConsumerClient;
import com.chaoppo.flink.app.flinkeventhub.model.EventData;
import com.chaoppo.flink.app.flinkeventhub.store.FlinkZookeeperStateStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class MessageSourceFunction implements SourceFunction<EventData> {

    private static final String SEPARATOR = "/";
    private static final long serialVersionUID = 6128016096756071380L;
    FlinkEventHubConsumerClient flinkEventHubConsumerClient;
    String zookeeperHost;
    Boolean isAutoCheckpointEnable;
    Integer parallelism;
    private volatile boolean isRunning = true;
    private String partitionId;
    private transient FlinkZookeeperStateStore flinkZookeeperStateStore;
    private transient EventHubConsumerClient eventHubConsumerClient;

    public MessageSourceFunction(FlinkEventHubConsumerClient flinkEventHubConsumerClient, String zookeeperHost,
            String partitionId, Boolean isAutoCheckpointEnable) {
        this.zookeeperHost = zookeeperHost;
        this.flinkEventHubConsumerClient = flinkEventHubConsumerClient;
        this.partitionId = partitionId;
        this.isAutoCheckpointEnable = isAutoCheckpointEnable;
        this.parallelism = Integer.parseInt(partitionId);
    }

    public MessageSourceFunction(FlinkEventHubConsumerClient flinkEventHubConsumerClient, String zookeeperHost,
            String partitionId, Integer parallelism, Boolean isAutoCheckpointEnable) {
        this.zookeeperHost = zookeeperHost;
        this.flinkEventHubConsumerClient = flinkEventHubConsumerClient;
        this.partitionId = partitionId;
        this.isAutoCheckpointEnable = isAutoCheckpointEnable;
        this.parallelism = parallelism;
    }

    public static void main(String arg[]) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        FlinkEventHubConsumerClient flinkEventHubConsumerClient = new FlinkEventHubConsumerClient(
                "Endpoint=sb://inbound01.servicebus.windows.net/;SharedAccessKeyName=ListenOnly;SharedAccessKey=CfBn5Yc5h2if1w3gfnjlhAPbRzzy46+5JwfcjBARrJQ=;EntityPath=ddeceventhub01;",
                "$DEFAULT");

        MessageSourceFunction messageSourceFunction = new MessageSourceFunction(flinkEventHubConsumerClient,
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

    @Override
    public void run(SourceContext<EventData> sourceContext) throws Exception {
        EventPosition eventPosition = null;
        flinkZookeeperStateStore = new FlinkZookeeperStateStore(zookeeperHost);
        flinkZookeeperStateStore.open();
        eventHubConsumerClient = flinkEventHubConsumerClient.createEventHubConsumerClient();
        eventPosition = getEventPosition();
        String partitionStatePath;

        IterableStream<PartitionEvent> events = eventHubConsumerClient
                .receiveFromPartition(partitionId, 999999999, eventPosition, Duration.ofSeconds(999999999));
        for (PartitionEvent partitionEvent : events) {
            System.out.println(partitionEvent.getData().getProperties());
            EventData eventData = getEventData(partitionEvent);

            if (isRunning) {
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(eventData);
                }
            }
            if (isAutoCheckpointEnable) {
                partitionStatePath = preparePartitionStatePath(partitionEvent);
                System.out.println("Save partitionStatePath" + partitionStatePath);
                flinkZookeeperStateStore
                        .saveData(partitionStatePath, partitionEvent.getData().getSequenceNumber().toString());
            }
        }
    }

    private EventPosition getEventPosition() {
        EventPosition eventPosition;
        String partitionStatePath =
                SEPARATOR + eventHubConsumerClient.getFullyQualifiedNamespace() + SEPARATOR + eventHubConsumerClient
                        .getEventHubName() + SEPARATOR + partitionId;
        List<String> offsets = flinkZookeeperStateStore.getChildrenBuilder(partitionStatePath);
        if (offsets != null && !offsets.isEmpty()) {
            List<Integer> listOffset = new ArrayList<>();
            for (String s : offsets) {
                listOffset.add(Integer.parseInt(s));
            }
            if (parallelism > 1) {
                eventPosition = EventPosition.fromSequenceNumber((Collections.max(listOffset) - parallelism) + 1, true);
            } else {
                eventPosition = EventPosition.fromSequenceNumber(Collections.max(listOffset) + 1, true);
            }
        } else {
            eventPosition = EventPosition.latest();
        }
        return eventPosition;
    }

    private EventData getEventData(PartitionEvent partitionEvent) {
        EventData eventData = new EventData(true);
        eventData.setFullyQualifiedNamespace(partitionEvent.getPartitionContext().getFullyQualifiedNamespace());
        eventData.setData(partitionEvent.getData().getBodyAsString().getBytes());
        eventData.setPartitionId(String.valueOf(partitionEvent.getPartitionContext().getPartitionId()));
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
                .append(partitionEvent.getPartitionContext().getPartitionId()).append(SEPARATOR)
                .append(partitionEvent.getData().getSequenceNumber()).append(SEPARATOR).append("state").toString()
                .toLowerCase(Locale.ROOT);
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (isAutoCheckpointEnable) {
            flinkZookeeperStateStore.close();
        }
    }

    @Override
    public String toString() {
        return flinkZookeeperStateStore.toString();
    }

    public String saveOffset(String fullyQualifiedNamespace, String eventHubName, String partitionId, String offset) {
        if (flinkZookeeperStateStore == null) {
            flinkZookeeperStateStore = new FlinkZookeeperStateStore(zookeeperHost);
            flinkZookeeperStateStore.open();
        }
        String partitionStatePath = preparePartitionStatePath(fullyQualifiedNamespace, eventHubName, partitionId,
                offset);
        flinkZookeeperStateStore.saveData(partitionStatePath, offset);
        return flinkZookeeperStateStore.readData(partitionStatePath);
    }

    private static String preparePartitionStatePath(String fullyQualifiedNamespace, String eventHubName,
            String partitionId, String offset) {
        // Partition state path =
        // "/{prefix}/{topologyName}/{namespace}/{entityPath}/partitions/{partitionId}/state";
        return new StringBuilder().append(SEPARATOR).append(fullyQualifiedNamespace).append(SEPARATOR)
                .append(eventHubName).append(SEPARATOR).append(partitionId).append(SEPARATOR).append(offset)
                .append(SEPARATOR).append("state").toString().toLowerCase(Locale.ROOT);
    }
}
