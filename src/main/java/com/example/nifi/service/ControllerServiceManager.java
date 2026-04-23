package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.config.FlowProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ControllerServiceManager {

    private static final Logger log = LoggerFactory.getLogger(ControllerServiceManager.class);

    private final NiFiClient client;

    public ControllerServiceManager(NiFiClient client) {
        this.client = client;
    }

    // =========================
    // COMMON SETUP METHOD
    // =========================
    private String setup(String token, String pgId, String type, String propsJson) {

        try {
            log.info("⚙️ Creating Controller Service: {}", type);

            // 1. CREATE
            String id = client.createCS(token, pgId, type);
            log.info("📌 Controller Service created with ID={}", id);

            // 2. GET VERSION
            int version = client.getVersion(token, id, "controller-services");

            // 3. UPDATE PROPERTIES
            log.info("🔧 Updating Controller Service: {}", id);
            client.updateCS(token, id, version, propsJson);

            // 4. GET LATEST VERSION
            int newVersion = client.getVersion(token, id, "controller-services");

            // 5. ENABLE
            log.info("⚡ Enabling Controller Service: {}", id);
            client.enable(token, id, newVersion);

            log.info("✅ Controller Service ready: {}", id);

            return id;

        } catch (Exception e) {
            log.error("❌ Failed to setup Controller Service: {}", type, e);
            throw new RuntimeException("Controller Service setup failed: " + type, e);
        }
    }

    // =========================
    // DBCP
    // =========================
    public String createDbcp(String token, String pgId, FlowProperties flow) {

        try {
            log.info("🗄️ Creating DBCP Controller Service...");

            String props = """
            {
              "Database Connection URL":"%s",
              "Database Driver Class Name":"%s",
              "Database Driver Locations":"%s",
              "Database User":"%s",
              "Password":"%s"
            }
            """.formatted(
                    flow.getDbcp().getUrl(),
                    flow.getDbcp().getDriverClass(),
                    flow.getDbcp().getDriverLocation(),
                    flow.getDbcp().getUser(),
                    flow.getDbcp().getPassword()
            );

            return setup(token, pgId,
                    "org.apache.nifi.dbcp.DBCPConnectionPool",
                    props);

        } catch (Exception e) {
            log.error("❌ Failed to create DBCP service", e);
            throw new RuntimeException("DBCP creation failed", e);
        }
    }

    // =========================
    // JSON WRITER
    // =========================
    public String createJsonWriter(String token, String pgId) {

        try {
            log.info("📝 Creating JSON Writer Controller Service...");

            return setup(token, pgId,
                    "org.apache.nifi.json.JsonRecordSetWriter",
                    "{}");

        } catch (Exception e) {
            log.error("❌ Failed to create JSON Writer", e);
            throw new RuntimeException("JSON Writer creation failed", e);
        }
    }

    // =========================
    // JSON READER
    // =========================
    public String createJsonReader(String token, String pgId) {

        try {
            log.info("📖 Creating JSON Reader Controller Service...");

            String props = """
            {
              "Schema Access Strategy": "infer-schema"
            }
            """;

            return setup(token, pgId,
                    "org.apache.nifi.json.JsonTreeReader",
                    props);

        } catch (Exception e) {
            log.error("❌ Failed to create JSON Reader", e);
            throw new RuntimeException("JSON Reader creation failed", e);
        }
    }

    // =========================
    // MONGO
    // =========================
    public String createMongo(String token, String pgId, FlowProperties flow) {

        try {
            log.info("🍃 Creating Mongo Controller Service...");

            String props = """
            {
              "Mongo URI":"%s",
              "Database User":"%s",
              "Password":"%s"
            }
            """.formatted(
                    flow.getMongo().getUri(),
                    flow.getMongo().getUser(),
                    flow.getMongo().getPassword()
            );

            return setup(token, pgId,
                    "org.apache.nifi.mongodb.MongoDBControllerService",
                    props);

        } catch (Exception e) {
            log.error("❌ Failed to create Mongo service", e);
            throw new RuntimeException("Mongo service creation failed", e);
        }
    }
}