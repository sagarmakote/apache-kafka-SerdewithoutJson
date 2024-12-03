package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetingsStream = streamsBuilder.stream(ALPHABETS, Consumed.with(Serdes.String(),Serdes.String()));
        greetingsStream.print(Printed.<String, String>toSysOut().withLabel("alphabetsStream"));

        var modifiedStream=greetingsStream.filter((key, value) -> value.length()>5)
                .mapValues((readOnlyKey, value)-> value.toUpperCase());
        modifiedStream.to(ALPHABETS_ABBREVATIONS, Produced.with(Serdes.String(),Serdes.String()));
        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedalphabetsStream"));


        return streamsBuilder.build();
    }

}
