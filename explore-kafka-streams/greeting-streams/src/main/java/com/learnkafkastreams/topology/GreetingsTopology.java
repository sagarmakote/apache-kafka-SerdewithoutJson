package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.Topology;

public class GreetingsTopology {

	public static String GREETINGS = "greetings";

	public static String GREETINGS_UPPERCASE = "greetings_uppercase";

	public static Topology buildTopology() {
		
		StreamsBuilder streamsBuilder= new StreamsBuilder();
		var greetingsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(),Serdes.String()));
		greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

		var modifiedStream=greetingsStream.filter((key, value) -> value.length()>5)
		.mapValues((readOnlyKey, value)-> value.toUpperCase());
		modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),Serdes.String()));
		modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));
		return streamsBuilder.build();
	}

}