package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import com.example.nifi.config.FlowProperties;
import com.example.nifi.config.NiFiProperties;
import org.springframework.stereotype.Service;

@Service
public class FlowBuilderService {

    private final NiFiClient client;
    private final ControllerServiceManager cs;
    private final FlowProperties flow;
    private final NiFiProperties nifi;
    private final ProcessorManager pm;

    public FlowBuilderService(NiFiClient client,
                              ControllerServiceManager cs,
                              FlowProperties flow,
                              NiFiProperties nifi,
                              ProcessorManager pm) {
        this.client = client;
        this.cs = cs;
        this.flow = flow;
        this.nifi = nifi;
        this.pm = pm;
    }

    public String buildFlow(String name) {

        String token = client.getToken(); // ✅ ONLY ONCE

        String pgId = client.createPG(token, nifi.getRootGroupId(), name);

        String dbProps = """
        {
          "Database Connection URL":"%s",
          "Database Driver Class Name":"%s",
          "Database Driver Locations":"%s",
          "Database User":"%s",
          "Password":"%s"
        }
        """.formatted(
                flow.dbcp.url,
                flow.dbcp.driverClass,
                flow.dbcp.driverLocation,
                flow.dbcp.user,
                flow.dbcp.password
        );

        String dbcp = cs.setup(token, pgId,
                "org.apache.nifi.dbcp.DBCPConnectionPool",
                dbProps);

        String writer = cs.setup(token, pgId,
                "org.apache.nifi.json.JsonRecordSetWriter",
                "{}");

        String reader = cs.setup(token, pgId,
                "org.apache.nifi.json.JsonTreeReader",
                """
                {
                  "Schema Access Strategy": "infer-schema"
                }
                """);

        String mongo = cs.setup(token, pgId,
                "org.apache.nifi.mongodb.MongoDBControllerService",
                """
                {
                  "Mongo URI":"%s",
                  "Database User":"%s",
                  "Password":"%s"
                }
                """.formatted(flow.mongo.uri, flow.mongo.user, flow.mongo.password));

        String query = pm.createQueryProcessor(token, pgId, dbcp, writer);
        String putMongo = pm.createPutMongoProcessor(token, pgId, mongo, reader);

        client.connect(token, pgId, query, putMongo);

        return "✅ FULL FLOW CREATED SUCCESSFULLY";
    }
}