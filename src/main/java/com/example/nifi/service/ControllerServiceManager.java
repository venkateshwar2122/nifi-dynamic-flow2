package com.example.nifi.service;

import com.example.nifi.client.NiFiClient;
import org.springframework.stereotype.Service;

@Service
public class ControllerServiceManager {

    private final NiFiClient client;

    public ControllerServiceManager(NiFiClient client) {
        this.client = client;
    }

    public String setup(String token, String pgId, String type, String props) {

        String id = client.createCS(token, pgId, type);

        int version = client.getVersion(token, id, "controller-services");

        client.updateCS(token, id, version, props);

        String reqId = client.verify(token, id, props);

        boolean ok = client.poll(token, id, reqId);

        if (!ok) {
            throw new RuntimeException("❌ Verification failed for " + type);
        }

        int newVersion = client.getVersion(token, id, "controller-services");

        client.enable(token, id, newVersion);

        return id;
    }
}