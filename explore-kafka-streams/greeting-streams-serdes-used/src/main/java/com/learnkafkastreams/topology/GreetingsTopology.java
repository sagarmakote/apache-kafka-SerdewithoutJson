package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.Topology;

public class GreetingsTopology {

	public static String GREETINGS = "greetings";
	public static String GREETINGS_UPPERCASE = "greetings_uppercase";
	public static String GREETINGS_SPANISH = "greetings_spanish";

	public static Topology buildTopology() {
		
		StreamsBuilder streamsBuilder= new StreamsBuilder();

//		var mergedStream= getStringGreetingKStream(streamsBuilder);
//		var modifiedStream=mergedStream.filter((key, value) -> value.length()>5)
//				.mapValues((readOnlyKey, value)-> value.toUpperCase());
//		modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),Serdes.String()));
//		modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

		var mergedStream = getCustomGreetingKStream(streamsBuilder);
		var modifiedStream=mergedStream.mapValues((readOnlyKey, value)->
				new Greeting(value.message().toUpperCase(), value.timeStamp()));
		modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),
				SerdesFactory.greetingSerdes()));
		modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

		return streamsBuilder.build();
	}

	private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
		KStream<String , String> greetingsStream = streamsBuilder.stream(GREETINGS);
		greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));
		KStream<String , String> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH);
		var mergedStream = greetingsStream.merge(greetingsSpanishStream);
		mergedStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));
		return mergedStream;
	}

	private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
		var greetingsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));
		System.out.println("greetingsStream------------------>"+greetingsStream.toString());
		var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));
		System.out.println("greetingsSpanishStream----------->"+greetingsSpanishStream.toString());
		var mergedStream = greetingsStream.merge(greetingsSpanishStream);
		System.out.println("mergedStream--------------------->"+mergedStream.toString());
		return mergedStream;
	}
}