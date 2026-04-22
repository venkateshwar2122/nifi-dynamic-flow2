package com.example.nifi.client;

import com.example.nifi.config.NiFiProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

@Component
public class NiFiClient {

    private final RestTemplate restTemplate;
    private final NiFiProperties props;
    private final ObjectMapper mapper = new ObjectMapper();

    public NiFiClient(RestTemplate restTemplate, NiFiProperties props) {
        this.restTemplate = restTemplate;
        this.props = props;
    }

    // 🔐 TOKEN
    public String getToken() {
    String url = props.getBaseUrl() + "/access/token";

    MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
    body.add("username", props.getUsername());
    body.add("password", props.getPassword());

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

    HttpEntity<MultiValueMap<String, String>> request =
            new HttpEntity<>(body, headers);

    return restTemplate.postForObject(url, request, String.class);
}

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        h.setBearerAuth(token);
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    // 📁 CREATE PG
    public String createPG(String token, String rootId, String name) {
        String body = """
        {
          "revision":{"version":0},
          "component":{"name":"%s"}
        }
        """.formatted(name);

        String res = restTemplate.postForObject(
            props.getBaseUrl() + "/process-groups/" + rootId + "/process-groups",
            new HttpEntity<>(body, headers(token)),
            String.class
        );

        return extractId(res);
    }

    // 🧠 COMMON METHODS
    private String extractId(String res) {
        try {
            return mapper.readTree(res).get("component").get("id").asText();
        } catch (Exception e) {
            throw new RuntimeException(e);
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

            return mapper.readTree(res).get("revision").get("version").asInt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 🔧 CREATE CONTROLLER SERVICE
    public String createCS(String token, String pgId, String type) {
        String body = """
        {
          "revision":{"version":0},
          "component":{"type":"%s"}
        }
        """.formatted(type);

        String res = restTemplate.postForObject(
            props.getBaseUrl() + "/process-groups/" + pgId + "/controller-services",
            new HttpEntity<>(body, headers(token)),
            String.class
        );

        return extractId(res);
    }

    // 🔧 UPDATE SERVICE
    public void updateCS(String token, String id, int version, String propsJson) {
        String body = """
        {
          "revision":{"version":%d},
          "component":{"id":"%s","properties":%s}
        }
        """.formatted(version, id, propsJson);

        restTemplate.exchange(
            props.getBaseUrl() + "/controller-services/" + id,
            HttpMethod.PUT,
            new HttpEntity<>(body, headers(token)),
            String.class
        );
    }

    // 🔍 VERIFY
    public String verify(String token, String id, String propsJson) {
        String body = """
        { "properties": %s }
        """.formatted(propsJson);

        String res = restTemplate.postForObject(
            props.getBaseUrl() + "/controller-services/" + id + "/config/verification-requests",
            new HttpEntity<>(body, headers(token)),
            String.class
        );

        try {
            return mapper.readTree(res).get("request").get("requestId").asText();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 🔁 POLL
    public boolean poll(String token, String id, String reqId) {
        try {
            while (true) {
                String res = restTemplate.exchange(
                    props.getBaseUrl() + "/controller-services/" + id + "/config/verification-requests/" + reqId,
                    HttpMethod.GET,
                    new HttpEntity<>(headers(token)),
                    String.class
                ).getBody();

                JsonNode json = mapper.readTree(res);

                if (json.get("request").get("complete").asBoolean()) {
                    String outcome = json.get("request")
                            .get("results").get(0)
                            .get("outcome").asText();

                    return outcome.equalsIgnoreCase("SUCCESSFUL");
                }

                Thread.sleep(1000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ⚡ ENABLE SERVICE
    public void enable(String token, String id, int version) {
        String body = """
        {
          "revision":{"version":%d},
          "state":"ENABLED"
        }
        """.formatted(version);

        restTemplate.exchange(
            props.getBaseUrl() + "/controller-services/" + id + "/run-status",
            HttpMethod.PUT,
            new HttpEntity<>(body, headers(token)),
            String.class
        );
    }

    public String createProcessor(String token, String pgId, String type) {

    String body = """
    {
      "revision":{"version":0},
      "component":{
        "type":"%s",
        "position":{"x":0.0,"y":0.0}
      }
    }
    """.formatted(type);

    String res = restTemplate.postForObject(
        props.getBaseUrl() + "/process-groups/" + pgId + "/processors",
        new HttpEntity<>(body, headers(token)),
        String.class
    );

    return extractId(res);
   }


   public void updateProcessor(String token,
                            String id,
                            int version,
                            String propertiesJson,
                            String schedule,
                            String autoTerminate) {

    String body = """
    {
      "revision":{"version":%d},
      "component":{
        "id":"%s",
        "config":{
          "properties": %s,
          "schedulingPeriod": "%s",
          "autoTerminatedRelationships": %s
        }
      }
    }
    """.formatted(version, id, propertiesJson, schedule, autoTerminate);

    restTemplate.exchange(
        props.getBaseUrl() + "/processors/" + id,
        HttpMethod.PUT,
        new HttpEntity<>(body, headers(token)),
        String.class
    );
}
public void connect(String token, String pgId, String sourceId, String destId) {

    String body = """
    {
      "revision":{"version":0},
      "component":{
        "source":{"id":"%s","type":"PROCESSOR"},
        "destination":{"id":"%s","type":"PROCESSOR"},
        "selectedRelationships":["success"]
      }
    }
    """.formatted(sourceId, destId);

    restTemplate.postForObject(
        props.getBaseUrl() + "/process-groups/" + pgId + "/connections",
        new HttpEntity<>(body, headers(token)),
        String.class
    );
}

private String safePost(String url, HttpEntity<?> entity) {
    try {
        return restTemplate.postForObject(url, entity, String.class);
    } catch (Exception e) {
        System.out.println("❌ ERROR: " + e.getMessage());
        throw e;
    }
}

}
