package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class GreetingDeserializer implements Deserializer<Greeting> {
    private ObjectMapper objectMapper;

    public GreetingDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Greeting deserialize(String topic, byte[] data) {
        try {
            System.out.println("==============="+data);
            //String str1 = new String(data);
            return objectMapper.readValue(data, Greeting.class);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
