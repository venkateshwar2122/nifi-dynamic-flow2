package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.config.FlowProperties;
import org.springframework.stereotype.Service;

@Service
public class ProcessorManager {

    private final NiFiClient client;
    private final FlowProperties flow;

    public ProcessorManager(NiFiClient client, FlowProperties flow) {
        this.client = client;
        this.flow = flow;
    }

    public String createQueryProcessor(String token, String pgId, String dbcpId, String writerId) {

        String processorId = client.createProcessor(
                token,
                pgId,
                "org.apache.nifi.processors.standard.QueryDatabaseTableRecord"
        );

        int version = client.getVersion(token, processorId, "processors");

        String props = """
        {
          "Database Connection Pooling Service": "%s",
          "Table Name": "%s",
          "Record Writer": "%s",
          "Maximum-value Columns": "%s"
        }
        """.formatted(
                dbcpId,
                flow.query.table,
                writerId,
                flow.query.pk
        );

        client.updateProcessor(token, processorId, version, props, flow.scheduling.query, "[]");

        return processorId;
    }

    public String createPutMongoProcessor(String token, String pgId, String mongoServiceId, String readerId) {

        String processorId = client.createProcessor(
                token,
                pgId,
                "org.apache.nifi.processors.mongodb.PutMongoRecord"
        );

        int version = client.getVersion(token, processorId, "processors");

        String props = """
        {
          "Client Service": "%s",
          "Mongo Database Name": "%s",
          "Mongo Collection Name": "%s",
          "Record Reader": "%s",
          "Update Key Fields": "%s"
        }
        """.formatted(
                mongoServiceId,
                flow.mongo.db,
                flow.mongo.collection,
                readerId,
                flow.query.pk
        );

        client.updateProcessor(token, processorId, version, props, "0 sec", "[]");

        return processorId;
    }
}