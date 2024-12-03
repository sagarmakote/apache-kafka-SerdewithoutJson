package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GreetingSerializer implements Serializer<Greeting> {
    private   ObjectMapper objectMapper;

    public GreetingSerializer(ObjectMapper objectMapper) {
        this.objectMapper=objectMapper;
    }

    @Override
    public byte[] serialize(String topic, Greeting data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
