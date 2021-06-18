package com.chaoppo.flink.app.flinkeventhub.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class EventData implements KafkaDeserializationSchema<EventData> {

    static Set<String> RESERVED_SYSTEM_PROPERTIES = null;
    private static long serialVersionUID = -2827050124966993723L;
    private boolean includeMetadata;
    private Map<String, Object> annotations = new LinkedHashMap<>();
    private byte[] data;

    private Long offset;
    private String partitionKey;
    private Instant enqueuedTime;
    private Long sequenceNumber;

    private String fullyQualifiedNamespace;
    private String partitionId;
    private String eventHubName;
    private String consumerGroup;

    public EventData(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    public EventData() {
    }

    public static Set<String> getReservedSystemProperties() {
        return RESERVED_SYSTEM_PROPERTIES;
    }

    public static void setReservedSystemProperties(Set<String> reservedSystemProperties) {
        RESERVED_SYSTEM_PROPERTIES = reservedSystemProperties;
    }

    public EventData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        EventData eventData = new EventData(true);

        if (record.value() != null) {
            eventData.data = record.value();
        }

        if (this.includeMetadata) {
            eventData.setOffset(record.offset());
            eventData.setEventHubName(record.topic());
            eventData.setPartitionId(String.valueOf(record.partition()));
            eventData.annotations.put("offset", record.offset());
            eventData.annotations.put("topic", record.topic());
            eventData.annotations.put("partition", record.partition());
        }

        return eventData;
    }

    public boolean isEndOfStream(EventData nextElement) {
        return false;
    }

    public TypeInformation<EventData> getProducedType() {
        return TypeExtractor.getForClass(EventData.class);
    }

    public String getAnnotation(String key) {
        if (!annotations.containsKey(key)) {
            return null;
        }
        return annotations.get(key).toString();
    }

    public void setAnnotation(String key, Object value) {
        if (value != null) {
            annotations.put(key, value);
        }
    }

    public boolean isIncludeMetadata() {
        return includeMetadata;
    }

    public void setIncludeMetadata(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public Instant getEnqueuedTime() {
        return enqueuedTime;
    }

    public void setEnqueuedTime(Instant enqueuedTime) {
        this.enqueuedTime = enqueuedTime;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getFullyQualifiedNamespace() {
        return fullyQualifiedNamespace;
    }

    public void setFullyQualifiedNamespace(String fullyQualifiedNamespace) {
        this.fullyQualifiedNamespace = fullyQualifiedNamespace;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public String getEventHubName() {
        return eventHubName;
    }

    public void setEventHubName(String eventHubName) {
        this.eventHubName = eventHubName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

}
