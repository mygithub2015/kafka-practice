package com.manish.kafkastream;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BankBalanceKafkaStreamAppApplication {

public static void main(String[] args) {
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG	, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		//we disable the cache to show all the transformation steps - not recommended in prod
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE );
		
		//json serdes
		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
		final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
		final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		
		
		KStreamBuilder streamsBuilder = new KStreamBuilder();
		KStream<String, JsonNode> bankTransactions = streamsBuilder.stream(Serdes.String(), jsonSerde,"bank-transactions");
		ObjectNode initialOutputNode = JsonNodeFactory.instance.objectNode();
		initialOutputNode.put("count", 0);
		initialOutputNode.put("balance", 0);
		initialOutputNode.put("time", Instant.ofEpochMilli(0).toString());
		
		KTable<String, JsonNode> bankBalance = bankTransactions
		.groupByKey(Serdes.String(), jsonSerde)
		//.reduce((balance, newTransaction)->newOutputNode(newTransaction, balance));
		.aggregate(
				()->initialOutputNode, 
				(key, newTransaction, balance)->newOutputNode(newTransaction, balance),
				jsonSerde,
				"bank-balance-agg"
				);
//		ObjectNode node = JsonNodeFactory.instance.objectNode();
//		node.put("name", "manish");
//		node.put("amount", 123);
//		node.put("time", LocalDateTime.now().toString());
//		
//		System.out.println(fetchedNode.get("name"));
		
		//write to output topic
		bankBalance.to(Serdes.String(), jsonSerde,"bank-balance-exactly-once");
		final KafkaStreams streams = new KafkaStreams(streamsBuilder, config);
		streams.start();
		
		System.out.println(streams);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	
	private static JsonNode newOutputNode(JsonNode newTransaction, JsonNode balance) {
		// TODO Auto-generated method stub
		int oldCount = balance.get("count").asInt();
		int oldBalance = balance.get("balance").asInt();
		/*
		 * DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxx");
		 * LocalDateTime oldTime = LocalDateTime.parse(balance.get("time").asText(),
		 * fmt);
		 */
		long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
		long transactionEpoch = Instant.parse(newTransaction.get("time").asText()).toEpochMilli();
		Instant maxEpoch = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
		int newBalance = newTransaction.get("amount").asInt();
		newBalance += oldBalance;
		int newCount = oldCount + 1;
		
		ObjectNode newNode = JsonNodeFactory.instance.objectNode();
		newNode.put("count", newCount);
		newNode.put("balance", newBalance);
		newNode.put("time", maxEpoch.toString());
		
		return newNode;
		

	}

}
