package com.example.nifi.client;
import com.example.nifi.config.NiFiProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

@Component
public class NiFiClient {

    private static final Logger log = LoggerFactory.getLogger(NiFiClient.class);

    private final RestTemplate restTemplate;
    private final NiFiProperties props;
    private final ObjectMapper mapper = new ObjectMapper();

    public NiFiClient(RestTemplate restTemplate, NiFiProperties props) {
        this.restTemplate = restTemplate;
        this.props = props;
    }

    // ================= TOKEN =================
    public String getToken() {
        try {
            log.info("🔐 Fetching NiFi token...");

            String url = props.getBaseUrl() + "/access/token";

            MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
            body.add("username", props.getUsername());
            body.add("password", props.getPassword());

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            String token = restTemplate.postForObject(
                    url, new HttpEntity<>(body, headers), String.class);

            log.info("✅ Token fetched successfully");
            return token;

        } catch (Exception e) {
            log.error("❌ Failed to fetch token", e);
            throw new RuntimeException("NiFi token fetch failed", e);
        }
    }

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        h.setBearerAuth(token);
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    // ================= COMMON =================
    private String extractId(String response) {
        try {
            JsonNode node = mapper.readTree(response);

            if (node == null || node.get("component") == null) {
                throw new RuntimeException("Invalid NiFi response: " + response);
            }

            return node.get("component").get("id").asText();

        } catch (Exception e) {
            log.error("❌ Failed to extract ID", e);
            throw new RuntimeException("ID extraction failed", e);
        }
    }

    public int getVersion(String token, String id, String type) {
        try {
            String res = restTemplate.exchange(
                    props.getBaseUrl() + "/" + type + "/" + id,
                    HttpMethod.GET,
                    new HttpEntity<>(headers(token)),
                    String.class
            ).getBody();

            return mapper.readTree(res)
                    .get("revision")
                    .get("version")
                    .asInt();

        } catch (Exception e) {
            log.error("❌ Failed to fetch version for {}", id, e);
            throw new RuntimeException("Version fetch failed", e);
        }
    }

    // ================= PROCESS GROUP =================
    public String createPG(String token, String rootId, String name) {
        try {
            log.info("📁 Creating Process Group: {}", name);

            String body = """
            {"revision":{"version":0},"component":{"name":"%s"}}
            """.formatted(name);

            String res = restTemplate.postForObject(
                    props.getBaseUrl() + "/process-groups/" + rootId + "/process-groups",
                    new HttpEntity<>(body, headers(token)),
                    String.class
            );

            String id = extractId(res);
            log.info("✅ Process Group created: {}", id);

            return id;

        } catch (Exception e) {
            log.error("❌ Failed to create Process Group", e);
            throw new RuntimeException("Create PG failed", e);
        }
    }

    // ================= CONTROLLER SERVICE =================
    public String createCS(String token, String pgId, String type) {
        try {
            log.info("⚙️ Creating Controller Service: {}", type);

            String body = """
            {"revision":{"version":0},"component":{"type":"%s"}}
            """.formatted(type);

            String res = restTemplate.postForObject(
                    props.getBaseUrl() + "/process-groups/" + pgId + "/controller-services",
                    new HttpEntity<>(body, headers(token)),
                    String.class
            );

            return extractId(res);

        } catch (Exception e) {
            log.error("❌ Failed to create Controller Service", e);
            throw new RuntimeException(e);
        }
    }

    public void updateCS(String token, String id, int version, String propsJson) {
    try {
        log.info("🔧 Updating Controller Service: {}", id);

        String body = """
        {
          "revision": { "version": %d },
          "component": {
            "id": "%s",
            "properties": %s
          }
        }
        """.formatted(version, id, propsJson);

        log.info("📦 CS UPDATE BODY: {}", body);

        restTemplate.exchange(
                props.getBaseUrl() + "/controller-services/" + id,
                HttpMethod.PUT,
                new HttpEntity<>(body, headers(token)),
                String.class
        );

    } catch (Exception e) {
        log.error("❌ Failed to update Controller Service {}", id, e);
        throw new RuntimeException(e);
    }
}

    public void enable(String token, String id, int version) {
        try {
            log.info("⚡ Enabling Controller Service: {}", id);

            String body = """
            {"revision":{"version":%d},"state":"ENABLED"}
            """.formatted(version);

            restTemplate.exchange(
                    props.getBaseUrl() + "/controller-services/" + id + "/run-status",
                    HttpMethod.PUT,
                    new HttpEntity<>(body, headers(token)),
                    String.class
            );

        } catch (Exception e) {
            log.error("❌ Failed to enable service {}", id, e);
            throw new RuntimeException(e);
        }
    }

    // ================= PROCESSOR =================
    public String createProcessor(String token, String pgId, String type) {
        try {
            log.info("🔧 Creating Processor: {}", type);

            String body = """
            {"revision":{"version":0},"component":{"type":"%s","position":{"x":0.0,"y":0.0}}}
            """.formatted(type);

            String res = restTemplate.postForObject(
                    props.getBaseUrl() + "/process-groups/" + pgId + "/processors",
                    new HttpEntity<>(body, headers(token)),
                    String.class
            );

            return extractId(res);

        } catch (Exception e) {
            log.error("❌ Failed to create processor", e);
            throw new RuntimeException(e);
        }
    }

    public void updateProcessorFull(String token, String id, int version,
                                String propertiesJson, String schedule, String rel) {
    try {
        log.info("⚙️ Updating Processor: {}", id);

        String body = """
        {
          "revision":{"version":%d},
          "component":{
            "id":"%s",
            "config":{
              "properties":%s,
              "schedulingPeriod":"%s",
              "autoTerminatedRelationships":%s
            }
          }
        }
        """.formatted(version, id, propertiesJson, schedule, rel);

        restTemplate.exchange(
                props.getBaseUrl() + "/processors/" + id,
                HttpMethod.PUT,
                new HttpEntity<>(body, headers(token)),
                String.class
        );

    } catch (Exception e) {
        log.error("❌ Failed to update processor {}", id, e);
        throw new RuntimeException(e);
    }
    }

    // ================= CONNECTION =================
    public void connect(String token, String pgId, String src, String dest) {
        try {
            log.info("🔗 Connecting processors");

            String body = """
            {
              "revision":{"version":0},
              "component":{
                "parentGroupId":"%s",
                "source":{"id":"%s","type":"PROCESSOR","groupId":"%s"},
                "destination":{"id":"%s","type":"PROCESSOR","groupId":"%s"},
                "selectedRelationships":["success"]
              }
            }
            """.formatted(pgId, src, pgId, dest, pgId);

            restTemplate.postForObject(
                    props.getBaseUrl() + "/process-groups/" + pgId + "/connections",
                    new HttpEntity<>(body, headers(token)),
                    String.class
            );

        } catch (Exception e) {
            log.error("❌ Failed to connect processors", e);
            throw new RuntimeException(e);
        }
    }

    public void controlProcessGroup(String token, String pgId, String state) {
        try {
            log.info("▶️ Changing PG state to {}", state);

            String body = """
            {"id":"%s","state":"%s"}
            """.formatted(pgId, state);

            restTemplate.exchange(
                    props.getBaseUrl() + "/flow/process-groups/" + pgId,
                    HttpMethod.PUT,
                    new HttpEntity<>(body, headers(token)),
                    String.class
            );

        } catch (Exception e) {
            log.error("❌ Failed to control process group", e);
            throw new RuntimeException(e);
        }
    }
}