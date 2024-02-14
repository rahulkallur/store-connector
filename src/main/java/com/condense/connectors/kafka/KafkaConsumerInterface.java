package com.condense.connectors.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import com.condense.connectors.bigtable.BigTableWriter;

public interface KafkaConsumerInterface extends AutoCloseable{

	void startAndProcessData(BigTableWriter bigTableWriter)throws IOException, TimeoutException;;



}
