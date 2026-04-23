package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.config.FlowProperties;
import com.example.nifi.config.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FlowBuilderService {

    private static final Logger log = LoggerFactory.getLogger(FlowBuilderService.class);

    private final NiFiClient client;
    private final ControllerServiceManager cs;
    private final ProcessorManager pm;
    private final FlowProperties flow;
    private final NiFiProperties nifi;

    public FlowBuilderService(
            NiFiClient client,
            ControllerServiceManager cs,
            ProcessorManager pm,
            FlowProperties flow,
            NiFiProperties nifi) {

        this.client = client;
        this.cs = cs;
        this.pm = pm;
        this.flow = flow;
        this.nifi = nifi;
    }

    // 🚀 CREATE + START FLOW
    public String buildFlow(String name) {

        try {
            log.info("🚀 Starting flow creation for: {}", name);

            // 1️⃣ TOKEN
            log.info("🔐 Fetching token...");
            String token = client.getToken();

            // 2️⃣ CREATE PROCESS GROUP
            log.info("📁 Creating Process Group...");
            String pgId = client.createPG(token, nifi.getRootGroupId(), name);

            // 3️⃣ DBCP SERVICE
            log.info("⚙️ Creating DBCP service...");
            String dbcpId = cs.createDbcp(token, pgId, flow);

            // 4️⃣ JSON WRITER
            log.info("📝 Creating JSON Writer...");
            String writerId = cs.createJsonWriter(token, pgId);

            // 5️⃣ JSON READER
            log.info("📖 Creating JSON Reader...");
            String readerId = cs.createJsonReader(token, pgId);

            // 6️⃣ MONGO SERVICE
            log.info("🍃 Creating Mongo service...");
            String mongoId = cs.createMongo(token, pgId, flow);

            // 7️⃣ PROCESSORS
            log.info("🔧 Creating Query Processor...");
            String queryId = pm.createQueryProcessor(token, pgId, dbcpId, writerId);

            log.info("🔧 Creating PutMongo Processor...");
            String putMongoId = pm.createPutMongoProcessor(token, pgId, mongoId, readerId);

            // 8️⃣ CONNECT
            log.info("🔗 Connecting processors...");
            client.connect(token, pgId, queryId, putMongoId);

            // 9️⃣ START FLOW
            log.info("▶️ Starting Process Group...");
            client.controlProcessGroup(token, pgId, "RUNNING");

            log.info("✅ Flow created and started successfully. PG_ID={}", pgId);

            return "✅ FLOW CREATED & STARTED | PG_ID = " + pgId;

        } catch (Exception e) {
            log.error("❌ Flow creation failed for: {}", name, e);
            throw new RuntimeException("Flow creation failed: " + e.getMessage(), e);
        }
    }

    // ▶️ START
    public void startFlow(String pgId) {

        try {
            log.info("▶️ Starting flow for PG_ID={}", pgId);

            String token = client.getToken();
            client.controlProcessGroup(token, pgId, "RUNNING");

            log.info("✅ Flow started successfully: {}", pgId);

        } catch (Exception e) {
            log.error("❌ Failed to start flow: {}", pgId, e);
            throw new RuntimeException("Start flow failed: " + pgId, e);
        }
    }

    // ⛔ STOP
    public void stopFlow(String pgId) {

        try {
            log.info("⛔ Stopping flow for PG_ID={}", pgId);

            String token = client.getToken();
            client.controlProcessGroup(token, pgId, "STOPPED");

            log.info("✅ Flow stopped successfully: {}", pgId);

        } catch (Exception e) {
            log.error("❌ Failed to stop flow: {}", pgId, e);
            throw new RuntimeException("Stop flow failed: " + pgId, e);
        }
    }
}