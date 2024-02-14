package com.condense.connectors.bigtable;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;

public interface BigTableWriter extends AutoCloseable{

	void processAndInsertData(String data);

}
