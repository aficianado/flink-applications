package com.chaoppo.flink.app.flinkeventhub.sf;

import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.chaoppo.flink.app.flinkeventhub.eventhub.FlinkEventHubConsumerClient;
import com.chaoppo.flink.app.flinkeventhub.model.EventData;
import com.chaoppo.flink.app.flinkeventhub.store.HbaseStateStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Locale;

public class HBaseSourceFunction implements SourceFunction<EventData> {

    private static final Logger log = LoggerFactory.getLogger(HBaseSourceFunction.class);
    private static final String SEPARATOR = "/";
    private static final long serialVersionUID = 6128016096756071380L;
    FlinkEventHubConsumerClient flinkEventHubConsumerClient;
    String topologyName;
    Boolean isAutoCheckpointEnable;
    Integer parallelism;
    private volatile boolean isRunning = true;
    private String partitionId;
    private transient HbaseStateStore hbaseStateStore;
    private transient EventHubConsumerClient eventHubConsumerClient;

    public HBaseSourceFunction(FlinkEventHubConsumerClient flinkEventHubConsumerClient, String topologyName,
            String partitionId, Boolean isAutoCheckpointEnable) {
        this.topologyName = topologyName;
        this.flinkEventHubConsumerClient = flinkEventHubConsumerClient;
        this.partitionId = partitionId;
        this.isAutoCheckpointEnable = isAutoCheckpointEnable;
        this.parallelism = Integer.parseInt(partitionId);
    }

    public HBaseSourceFunction(FlinkEventHubConsumerClient flinkEventHubConsumerClient, String topologyName,
            String partitionId, Integer parallelism, Boolean isAutoCheckpointEnable) {
        this.topologyName = topologyName;
        this.flinkEventHubConsumerClient = flinkEventHubConsumerClient;
        this.partitionId = partitionId;
        this.isAutoCheckpointEnable = isAutoCheckpointEnable;
        this.parallelism = parallelism;
    }

    public static void main(String arg[]) throws Exception {

        //        System.out.println(Bytes.toString(24674613981672));
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        FlinkEventHubConsumerClient flinkEventHubConsumerClient = new FlinkEventHubConsumerClient(
                "Endpoint=sb://inbound01.servicebus.windows.net/;SharedAccessKeyName=ListenOnly;SharedAccessKey=CfBn5Yc5h2if1w3gfnjlhAPbRzzy46+5JwfcjBARrJQ=;EntityPath=ddeceventhub01;",
                "$DEFAULT");

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

    @Override
    public void run(SourceContext<EventData> sourceContext) throws Exception {
        log.info("Start HBaseSourceFunction");
        EventPosition eventPosition = null;
        hbaseStateStore = new HbaseStateStore(topologyName, "CONTROL");
        hbaseStateStore.open();
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
                hbaseStateStore.saveData(partitionStatePath, partitionEvent.getData().getSequenceNumber().toString());
                log.info("Saved partitionStatePath in HBase" + partitionStatePath);
            }
        }
    }

    private EventPosition getEventPosition() {
        EventPosition eventPosition;
        String partitionStatePath =
                SEPARATOR + eventHubConsumerClient.getFullyQualifiedNamespace() + SEPARATOR + eventHubConsumerClient
                        .getEventHubName() + SEPARATOR + "partitions" + SEPARATOR + partitionId;
        String offset = hbaseStateStore.readData(partitionStatePath);
        if (offset != null) {

            if (parallelism > 1) {
                eventPosition = EventPosition.fromSequenceNumber((Integer.parseInt(offset) - parallelism) + 1, true);
            } else {
                eventPosition = EventPosition.fromSequenceNumber(Integer.parseInt(offset) + 1, true);
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
                .append(partitionEvent.getPartitionContext().getEventHubName()).append(SEPARATOR).append("partitions")
                .append(SEPARATOR).append(partitionEvent.getPartitionContext().getPartitionId()).toString()
                .toLowerCase(Locale.ROOT);
    }

    public void setCommitOffsetsOnCheckpoints(boolean isAutoCheckpointEnable) {
        this.isAutoCheckpointEnable = isAutoCheckpointEnable;
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (isAutoCheckpointEnable) {
            hbaseStateStore.close();
        }
    }

    @Override
    public String toString() {
        return hbaseStateStore.toString();
    }

    public String saveOffset(String fullyQualifiedNamespace, String eventHubName, String partitionId, String offset) {
        if (hbaseStateStore == null) {
            hbaseStateStore = new HbaseStateStore(topologyName, "CONTROL");
            hbaseStateStore.open();
        }
        String partitionStatePath = preparePartitionStatePath(fullyQualifiedNamespace, eventHubName, partitionId,
                offset);
        hbaseStateStore.saveData(partitionStatePath, offset);
        return hbaseStateStore.readData(partitionStatePath);
    }

    private static String preparePartitionStatePath(String fullyQualifiedNamespace, String eventHubName,
            String partitionId, String offset) {
        // Partition state path =
        // "/{prefix}/{topologyName}/{namespace}/{entityPath}/partitions/{partitionId}/state";
        return new StringBuilder().append(SEPARATOR).append(fullyQualifiedNamespace).append(SEPARATOR)
                .append(eventHubName).append(SEPARATOR).append("partitions").append(SEPARATOR).append(partitionId)
                .toString().toLowerCase(Locale.ROOT);
    }
}
