package com.manish.kafkastream;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WordCountApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(WordCountApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		//word-count-problem
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG	, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, String> textLines = streamsBuilder.stream("word-count-input");
		System.out.println(textLines.toString());
		KTable<String, Long> wordCounts = textLines
										.mapValues(line->line.toLowerCase())
										.flatMapValues(line->Arrays.asList(line.split(" ")))
										.selectKey((key, word)->word)
										.groupByKey()
										.count(Materialized.as("Counts"));
		wordCounts.toStream().to("word-count-output");

		final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
		streams.start();
		
		System.out.println(streams);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
