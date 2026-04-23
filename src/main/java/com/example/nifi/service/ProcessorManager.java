package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.config.FlowProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProcessorManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessorManager.class);

    private final NiFiClient client;
    private final FlowProperties flow;

    public ProcessorManager(NiFiClient client, FlowProperties flow) {
        this.client = client;
        this.flow = flow;
    }

    // =========================
    // QUERY PROCESSOR
    // =========================
    public String createQueryProcessor(
            String token,
            String pgId,
            String dbcpId,
            String writerId) {

        try {
            log.info("🔧 Creating QueryDatabaseTableRecord processor...");

            String id = client.createProcessor(
                    token,
                    pgId,
                    "org.apache.nifi.processors.standard.QueryDatabaseTableRecord"
            );

            log.info("📌 Query Processor created with ID={}", id);

            int version = client.getVersion(token, id, "processors");

            String props = """
            {
              "Database Connection Pooling Service": "%s",
              "Table Name": "%s",
              "Record Writer": "%s",
              "Maximum-value Columns": "%s"
            }
            """.formatted(
                    dbcpId,
                    flow.getQuery().getTable(),
                    writerId,
                    flow.getQuery().getPk()
            );

            // 🔥 Auto terminate
            String relationships = "[\"failure\"]";

            log.info("⚙️ Updating Query Processor configuration...");

            client.updateProcessorFull(
                    token,
                    id,
                    version,
                    props,
                    flow.getScheduling().getQuery(),
                    relationships
            );

            log.info("✅ Query Processor configured successfully: {}", id);

            return id;

        } catch (Exception e) {
            log.error("❌ Failed to create Query Processor", e);
            throw new RuntimeException("Query Processor creation failed", e);
        }
    }

    // =========================
    // PUT MONGO PROCESSOR
    // =========================
    public String createPutMongoProcessor(
            String token,
            String pgId,
            String mongoId,
            String readerId) {

        try {
            log.info("🔧 Creating PutMongoRecord processor...");

            String id = client.createProcessor(
                    token,
                    pgId,
                    "org.apache.nifi.processors.mongodb.PutMongoRecord"
            );

            log.info("📌 PutMongo Processor created with ID={}", id);

            int version = client.getVersion(token, id, "processors");

            String props = """
            {
              "Client Service": "%s",
              "Mongo Database Name": "%s",
              "Mongo Collection Name": "%s",
              "Record Reader": "%s",
              "Update Key Fields": "%s"
            }
            """.formatted(
                    mongoId,
                    flow.getMongo().getDb(),
                    flow.getMongo().getCollection(),
                    readerId,
                    flow.getQuery().getPk()
            );

            String relationships = "[\"failure\", \"success\"]";

            log.info("⚙️ Updating PutMongo Processor configuration...");

            client.updateProcessorFull(
                    token,
                    id,
                    version,
                    props,
                    "0 sec",
                    relationships
            );

            log.info("✅ PutMongo Processor configured successfully: {}", id);

            return id;

        } catch (Exception e) {
            log.error("❌ Failed to create PutMongo Processor", e);
            throw new RuntimeException("PutMongo Processor creation failed", e);
        }
    }
}