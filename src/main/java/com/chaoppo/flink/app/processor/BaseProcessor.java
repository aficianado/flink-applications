package com.chaoppo.flink.app.processor;

import com.chaoppo.flink.app.api.Msg;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.common.functions.MapFunction;

public abstract class BaseProcessor<T> implements MapFunction<Msg, T> {

    ObjectMapper mapper = new ObjectMapper().configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    ObjectMapper objectMapper;

    public BaseProcessor() {
        this.objectMapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public byte[] decodeBase64(String data) {
        return Base64.decodeBase64(data);
    }

}
