package com.manish.kafkastream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FavouriteColorApplication implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(FavouriteColorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG	, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		//we disable the cache to show all the transformation steps - not recommended in prod
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		
		List<String> favouriteColors = Arrays.asList("blue", "green", "red");
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		//building streams where key is userId
		KStream<String, String> favouriteColorLines = streamsBuilder.stream("favourite-color-input-topic");
		favouriteColorLines = favouriteColorLines
			.filter((user, color)->color.contains(","))
			.selectKey((user, color)->color.split(",")[0].toLowerCase())
			.mapValues(color->color.split(",")[1].toLowerCase())
			.filter((user,color)->favouriteColors.contains(color));
//			.filter((key, value)->favouriteColors.stream().anyMatch(e->value.matches(e)));
		//writing to intermediate log compact topic
		favouriteColorLines.to("latest-favourite-color-topic");
		//
		 KTable<String, String> intermediateTable = streamsBuilder.table("latest-favourite-color-topic", Consumed.with(Serdes.String(), Serdes.String()));
		 //KStream<String, String> intermediateStream =  intermediateTable.toStream();
		 //final KTable
		 KTable<String, Long> favouriteColorsTable = intermediateTable
						.groupBy((user,color)->new KeyValue<>(color,color))
						.count(Materialized.as("color-counts"));
		//writing to output topic
		favouriteColorsTable.toStream().to("favourite-color-output-topic");

		KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
		streams.start();
		
		System.out.println(streams);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
