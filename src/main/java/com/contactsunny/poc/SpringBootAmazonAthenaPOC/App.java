package com.contactsunny.poc.SpringBootAmazonAthenaPOC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class App implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    private static final String ATHENA_DATABASE = "athenatest";

    private static final String ATHENA_OUTPUT_S3_FOLDER_PATH = "s3://athena-poc-contactsunny/";

    private static final String SIMPLE_ATHENA_QUERY = "select * from sampledata limit 10;";
    private static final long SLEEP_AMOUNT_IN_MS = 1000;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        AthenaClientFactory factory = new AthenaClientFactory();
        AthenaClient athenaClient = factory.createClient();

        String queryExecutionId = submitAthenaQuery(athenaClient);

        logger.info("Query submitted: " + System.currentTimeMillis());

        waitForQueryToComplete(athenaClient, queryExecutionId);

        logger.info("Query finished: " + System.currentTimeMillis());

        processResultRows(athenaClient, queryExecutionId);
    }

    private static String submitAthenaQuery(AthenaClient athenaClient) {

        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(ATHENA_DATABASE).build();

        ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(ATHENA_OUTPUT_S3_FOLDER_PATH).build();

        StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(SIMPLE_ATHENA_QUERY)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration).build();

        StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);

        return startQueryExecutionResponse.queryExecutionId();
    }

    private static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {

        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;

        boolean isQueryStillRunning = true;

        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();

            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                Thread.sleep(SLEEP_AMOUNT_IN_MS);
            }

            logger.info("Current Status is: " + queryState);
        }
    }

    private static void processResultRows(AthenaClient athenaClient, String queryExecutionId) {

        GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

        for (GetQueryResultsResponse Resultresult : getQueryResultsResults) {
            List<ColumnInfo> columnInfoList = Resultresult.resultSet().resultSetMetadata().columnInfo();

            int resultSize = Resultresult.resultSet().rows().size();
            logger.info("Result size: " + resultSize);

            List<Row> results = Resultresult.resultSet().rows();
            processRow(results, columnInfoList);
        }
    }

    private static void processRow(List<Row> rowList, List<ColumnInfo> columnInfoList) {

        List<String> columns = new ArrayList<>();

        for (ColumnInfo columnInfo : columnInfoList) {
            columns.add(columnInfo.name());
        }

        for (Row row: rowList) {
            int index = 0;

            for (Datum datum : row.data()) {
                logger.info(columns.get(index) + ": " + datum.varCharValue());
                index++;
            }

            logger.info("===================================");
        }
    }
}
