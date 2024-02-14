package com.condense.connectors.impl;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;


import com.condense.connectors.bigtable.BigTableWriter;
import com.condense.connectors.kafkabigtable.Main;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BigTableWriterImpl implements BigTableWriter {

    private static final Logger logger = LogManager.getLogger(BigTableWriterImpl.class);

    private static Properties properties;
    private static BigtableDataClient bigtableDataClient;
    private List<RowMutation> pendingMutations = new ArrayList<>();
    private String batchSize = Main.BIG_TABLE_BATCH_SIZE;
    private int currentBatchSize = 0;
    private List<String> jsonDataList = new ArrayList<>();
    private int batch_size = Integer.parseInt(batchSize);
   

    private BulkMutation bulkMutation;

    private String tableId = Main.BIGTABLE_TABLE_ID; 

    public BigTableWriterImpl(Properties properties, BigtableDataClient bigtableDataClient2) {
        this.properties = properties;
        this.bigtableDataClient = bigtableDataClient2;
    }

    public static BigtableDataClient getBigtableDataClient(Properties properties){
    try {
    	logger.info(("establishing bigtable data client connection"));
    	String bigTable_projectId = Main.BIGTABLE_PROJECT_ID;
    	String bigTable_instanceId = Main.BIGTABLE_INSTANCE_ID;
    	String bigTable_serviceAccountKey = Main.SERVICE_ACCOUNT_CREDENTIAL;
       
       byte[] decodedBytes = Base64.getDecoder().decode(bigTable_serviceAccountKey);

       String decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
      
       byte[] bytes = decodedString.toString().getBytes(StandardCharsets.UTF_8);

       
       ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
       
       GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream);

      
       BigtableDataSettings settings = BigtableDataSettings.newBuilder()
               .setProjectId(bigTable_projectId)
               .setInstanceId(bigTable_instanceId)
               .setCredentialsProvider(() -> credentials)
               .build();

       
       return BigtableDataClient.create(settings);
    }catch(Exception e) {
    e.printStackTrace();
    }
    return null;
    }

    @Override
    public void close() throws Exception {
        try {
            if (bigtableDataClient != null) {
                bigtableDataClient.close();
                logger.info("BigtableDataClient closed");
            }
        } catch (Exception e) {
            logger.error("Error closing BigtableDataClient", e);
        }
    }
    
    @Override
    public void processAndInsertData(String jsonData) {
    logger.info("inside the process And Insert Data");
        JsonArray dataArray = JsonParser.parseString(jsonData).getAsJsonArray();

        if (bulkMutation == null || currentBatchSize >= batch_size) {
            bulkMutation = BulkMutation.create(tableId);
            currentBatchSize = 0;  
            jsonDataList.clear();
        }
        
       

        for (JsonElement element : dataArray) {
            if (element.isJsonObject()) {
                JsonObject jsonObject = element.getAsJsonObject();
                String rowKey = jsonObject.getAsJsonPrimitive("key").getAsString();
                JsonObject data = jsonObject.getAsJsonObject("data");
                insertJsonDataIntoBigtable(data, rowKey, bulkMutation);
            }
        }

        currentBatchSize++;
        if (currentBatchSize >= batch_size) {
            logger.info("Inside the Bulk mutaion after satisfying batch size");
            executeBulkMutation(bulkMutation);
        }
    }
   

    
    private void insertJsonDataIntoBigtable(JsonObject data, String rowKey, BulkMutation bulkMutation) {
    logger.info("inside the insertjson data to bigtable from kafka");
    for (String columnFamily : data.keySet()) {
        JsonObject columnFamilyData = data.getAsJsonObject(columnFamily);

        for (String column : columnFamilyData.keySet()) {
            JsonObject qualifierData = columnFamilyData.getAsJsonObject(column);
            String value = qualifierData.getAsJsonPrimitive("value").getAsString();

            logger.info("Adding mutation. RowKey: {}, ColumnFamily: {}, Column: {}, Value: {}",
            	    rowKey, columnFamily, column, value);


            Mutation mutation = Mutation.create()
                    .setCell(columnFamily, ByteString.copyFromUtf8(column), ByteString.copyFromUtf8(value));

            try {
                bulkMutation.add(rowKey, mutation);
            } catch (Exception e) {
                logger.error("Error adding mutation. RowKey: {}, ColumnFamily: {}, Column: {}", rowKey, columnFamily, column);
                throw e;
            }
        }
    }
}

    
    
    
    private void executeBulkMutation(BulkMutation bulkMutation) {
    try {
        logger.info("Executing Bulk Mutation..");

        logger.info("Number of entries in BulkMutation: " + bulkMutation.getEntryCount());


        try {
            bigtableDataClient.bulkMutateRows(bulkMutation);
            System.out.println("Bulk mutation executed");
        } catch (MutateRowsException e) {
            logger.error("Error during bulk mutation:");
            for (MutateRowsException.FailedMutation failedMutation : e.getFailedMutations()) {
                System.out.println("Failed mutation status: " + failedMutation.getError().getStatusCode());
            }
        }
    } catch (Exception e) {
        logger.error("Error during bulk mutation: \n" + e.toString());
    }
}




}
