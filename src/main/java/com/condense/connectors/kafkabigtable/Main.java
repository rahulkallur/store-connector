package com.condense.connectors.kafkabigtable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.condense.connectors.bigtable.BigTableWriter;
import com.condense.connectors.impl.BigTableWriterImpl;
import com.condense.connectors.impl.KafkaConsumerImpl;
import com.condense.connectors.kafka.KafkaConsumerInterface;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;

public class Main{
private static final Logger logger = LogManager.getLogger(Main.class);
public static String KAFKA_BOOTSTRAP_SERVERS;
public static String KAFKA_CONSUMER_GROUP;
public static String KAFKA_TOPIC;
public static String BIGTABLE_PROJECT_ID;
public static String BIGTABLE_INSTANCE_ID;
public static String SERVICE_ACCOUNT_CREDENTIAL;
public static String BIGTABLE_TABLE_ID;
public static String BIGTABLE_ROW_KEY;
public static String BIGTABLE_FAMILY_NAME;
public static String BIG_TABLE_BATCH_SIZE;



public static void main(String[] args) throws IOException {
logger.info("inside the main file to check for yml or config properties");
	Properties properties = loadProperties();
	
	
	KAFKA_BOOTSTRAP_SERVERS = loadconfigfile("KAFKA_BOOTSTRAP_SERVERS", properties);
	KAFKA_CONSUMER_GROUP = loadconfigfile("KAFKA_CONSUMER_GROUP", properties);
	KAFKA_TOPIC = loadconfigfile("KAFKA_TOPIC", properties);
	BIGTABLE_PROJECT_ID = loadconfigfile("BIGTABLE_PROJECT_ID", properties);
	BIGTABLE_INSTANCE_ID = loadconfigfile("BIGTABLE_INSTANCE_ID", properties);
	SERVICE_ACCOUNT_CREDENTIAL = loadconfigfile("SERVICE_ACCOUNT_CREDENTIAL", properties);
	BIGTABLE_TABLE_ID = loadconfigfile("BIGTABLE_TABLE_ID", properties);
	//BIGTABLE_ROW_KEY = loadconfigfile("BIGTABLE_ROW_KEY", properties);
	//BIGTABLE_FAMILY_NAME = loadconfigfile("BIGTABLE_FAMILY_NAME", properties);
	BIG_TABLE_BATCH_SIZE = loadconfigfile("BIG_TABLE_BATCH_SIZE", properties);
	BigtableDataClient bigtableDataClient = BigTableWriterImpl.getBigtableDataClient(properties);
    KafkaConsumerInterface kafkaConsumer = new KafkaConsumerImpl(properties);
    BigTableWriter bigTableWriter = new BigTableWriterImpl(properties, bigtableDataClient);
    
    
    try {
    kafkaConsumer.startAndProcessData(bigTableWriter);
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
    try {
        kafkaConsumer.close();
        bigTableWriter.close();
    } catch (Exception e) {
        e.printStackTrace();
    }
}

}

private static Properties loadProperties() {
	Properties properties = new Properties();
	
	try (InputStream input = Main.class.getClassLoader().getResourceAsStream("config/config.properties")) {
	    if (input == null) {
	        logger.error("Unable to find the config.properties file.");
	        System.exit(1);
	    }
	    properties.load(input);
	} catch (IOException e) {
	    logger.error("Error loading config.properties file.", e);
	    System.exit(1);
	}

	return properties;
}

private static String loadconfigfile(String propertyName,Properties properties){
	String envVarValue = System.getenv(propertyName);
	
    return (envVarValue != null && !envVarValue.isEmpty()) ? envVarValue : properties.getProperty(propertyName);

}
}
