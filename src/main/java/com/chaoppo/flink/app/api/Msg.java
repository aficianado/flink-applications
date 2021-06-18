package com.chaoppo.flink.app.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.LinkedHashMap;
import java.util.Map;

public class Msg implements KafkaDeserializationSchema<Msg> {

    private static final long serialVersionUID = 1509391548173891955L;
    private final boolean includeMetadata;
    byte[] data;
    private Map<String, Object> annotations = new LinkedHashMap<>();

    public Msg(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    public Msg deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        Msg msg = new Msg(true);

        if (record.value() != null) {
            msg.data = record.value();
        }

        if (this.includeMetadata) {
            msg.annotations.put("offset", record.offset());
            msg.annotations.put("topic", record.topic());
            msg.annotations.put("partition", record.partition());
        }

        return msg;
    }

    public boolean isEndOfStream(Msg nextElement) {
        return false;
    }

    public TypeInformation<Msg> getProducedType() {
        return TypeExtractor.getForClass(Msg.class);
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isIncludeMetadata() {
        return includeMetadata;
    }

    public String getAnnotation(String key) {
        if (!annotations.containsKey(key)) {
            return null;
        }
        return annotations.get(key).toString();
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
    }

    public void setAnnotation(String key, Object value) {
        if (value != null) {
            annotations.put(key, value);
        }
    }

}