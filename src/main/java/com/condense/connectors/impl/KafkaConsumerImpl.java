package com.condense.connectors.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.condense.connectors.bigtable.BigTableWriter;
import com.condense.connectors.kafka.KafkaConsumerInterface;
import com.condense.connectors.kafkabigtable.Main;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

public class KafkaConsumerImpl implements KafkaConsumerInterface {
private static final Logger logger = LogManager.getLogger(KafkaConsumerImpl.class);
private static Properties properties; 
private Consumer<String, String> consumer;
private BigtableDataClient bigtableDataClient;


public KafkaConsumerImpl(Properties properties){
	this.properties=properties;
}

@Override
public void startAndProcessData(BigTableWriter bigTableWriter) {
	String brokerUrl = Main.KAFKA_BOOTSTRAP_SERVERS;
	String consumerGroup = Main.KAFKA_CONSUMER_GROUP;
	String kafkaTopic = Main.KAFKA_TOPIC;
	
	Properties kafkaProperties = new Properties();
	kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
	kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
	kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	
	try (Consumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties)) {
	    consumer.subscribe(Collections.singletonList(kafkaTopic));
	  logger.info("Connected to Kafka Broker...");
	
	    while (true) {

	        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

	        records.forEach(record -> {
	            String data = record.value();
	            logger.info("Record: {}", data);
	
	            try {
	            logger.info("from kafka to bigtable process and insert data ");
	            bigTableWriter.processAndInsertData(data);
	            } catch (Exception e) {
	                logger.error("Error processing and inserting data into Bigtable", e);
	            }
	        });
	    }
	} catch (Exception e) {
	    logger.error("Error in Kafka consumer", e);
	}
}

@Override public void close() throws Exception{
	try {
	if (consumer != null) {
	    consumer.close();
	    logger.info("KafkaConsumer closed");
	}
	} catch (Exception e) {
	logger.error("Error closing KafkaConsumer", e);
}

}

}

	

