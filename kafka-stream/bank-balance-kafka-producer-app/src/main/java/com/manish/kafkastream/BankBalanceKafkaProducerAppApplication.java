package com.manish.kafkastream;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BankBalanceKafkaProducerAppApplication implements CommandLineRunner{
	
	private final static String TOPIC = "bank-transactions";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all"); //strongest producing guarantee
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //ensure we don't have duplicates
        return new KafkaProducer<>(props);
    }
    
	/*
	 * static void runProducer(final int sendMessageCount) throws Exception { final
	 * Producer<Long, String> producer = createProducer(); long time =
	 * System.currentTimeMillis();
	 * 
	 * try { for (long index = time; index < time + sendMessageCount; index++) {
	 * final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC,
	 * index, "Hello Mom " + index);
	 * 
	 * RecordMetadata metadata = producer.send(record).get();
	 * 
	 * long elapsedTime = System.currentTimeMillis() - time;
	 * System.out.printf("sent record(key=%s value=%s) " +
	 * "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(),
	 * metadata.partition(), metadata.offset(), elapsedTime);
	 * 
	 * } } finally { producer.flush(); producer.close(); } }
	 */
    
    private static JSONObject getJsonObject(String name) {
    	JSONObject obj = new JSONObject();
      	int amount = new Random().nextInt(1000);
    	obj.put("name", name);
    	obj.put("amount", amount);
    	obj.put("time", Instant.now());
    	return obj;
    }
    private static JSONArray getJsonInputArray() {
    	JSONArray inputArr = new JSONArray();
    	inputArr.put(getJsonObject("John"));
    	inputArr.put(getJsonObject("Manish"));
    	inputArr.put(getJsonObject("Rakesh"));
    	inputArr.put(getJsonObject("Ankita"));
    	inputArr.put(getJsonObject("Stephane"));
    	inputArr.put(getJsonObject("Jitu"));

    	return inputArr;
    }
    
    private static JSONObject getRandomInput() {
    	Random random = new Random();
    	JSONArray arr = getJsonInputArray();
    	int index = random.nextInt(arr.length());
    	return arr.getJSONObject(index);
    }
    
    //producing records asynchronously
    static void runProducer(final int sendMessageCount) throws InterruptedException {
        final Producer<String, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
            	JSONObject obj = getRandomInput();
                final ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, obj.get("name").toString(), obj.toString());
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) " +
                                        "meta(partition=%d, offset=%d) time=%d\n",
                                record.key(), record.value(), metadata.partition(),
                                metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        }finally {
            producer.flush();
            producer.close();
        }
    }
    
	public static void main(String[] args) {
	
		SpringApplication.run(BankBalanceKafkaProducerAppApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		/*
		 * JSONArray arr = getJsonInputArray(); System.out.println(arr.get(0));
		 */
		
		runProducer(10);
	}

}
