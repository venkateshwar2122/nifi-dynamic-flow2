package com.example.nifi.controller;

import com.example.nifi.dto.FlowRequest;
import com.example.nifi.service.FlowBuilderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/flow")
public class FlowController {

    private static final Logger log = LoggerFactory.getLogger(FlowController.class);

    private final FlowBuilderService service;

    public FlowController(FlowBuilderService service) {
        this.service = service;
    }

    /**
     * 🚀 CREATE + START NEW FLOW
     */
    @PostMapping("/create")
    public ResponseEntity<?> createFlow(@RequestBody FlowRequest request) {

        try {
            log.info("📥 API CALL: Create Flow | name={}", request.getName());

            String result = service.buildFlow(request.getName());

            log.info("✅ Flow created successfully");
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("❌ Create Flow failed | name={}", request.getName(), e);
            return ResponseEntity.internalServerError()
                    .body("Flow creation failed: " + e.getMessage());
        }
    }

    /**
     * ▶️ START EXISTING FLOW
     */
    @PostMapping("/start/{pgId}")
    public ResponseEntity<?> startFlow(@PathVariable String pgId) {

        try {
            log.info("📥 API CALL: Start Flow | pgId={}", pgId);

            service.startFlow(pgId);

            log.info("✅ Flow started successfully: {}", pgId);
            return ResponseEntity.ok("▶️ Flow STARTED successfully for PG_ID = " + pgId);

        } catch (Exception e) {
            log.error("❌ Start Flow failed | pgId={}", pgId, e);
            return ResponseEntity.internalServerError()
                    .body("Start flow failed: " + e.getMessage());
        }
    }

    /**
     * ⛔ STOP EXISTING FLOW
     */
    @PostMapping("/stop/{pgId}")
    public ResponseEntity<?> stopFlow(@PathVariable String pgId) {

        try {
            log.info("📥 API CALL: Stop Flow | pgId={}", pgId);

            service.stopFlow(pgId);

            log.info("✅ Flow stopped successfully: {}", pgId);
            return ResponseEntity.ok("⛔ Flow STOPPED successfully for PG_ID = " + pgId);

        } catch (Exception e) {
            log.error("❌ Stop Flow failed | pgId={}", pgId, e);
            return ResponseEntity.internalServerError()
                    .body("Stop flow failed: " + e.getMessage());
        }
    }

    /**
     * 🔁 RESTART FLOW
     */
    @PostMapping("/restart/{pgId}")
    public ResponseEntity<?> restartFlow(@PathVariable String pgId) {

        try {
            log.info("📥 API CALL: Restart Flow | pgId={}", pgId);

            service.stopFlow(pgId);
            service.startFlow(pgId);

            log.info("✅ Flow restarted successfully: {}", pgId);
            return ResponseEntity.ok("🔁 Flow RESTARTED successfully for PG_ID = " + pgId);

        } catch (Exception e) {
            log.error("❌ Restart Flow failed | pgId={}", pgId, e);
            return ResponseEntity.internalServerError()
                    .body("Restart flow failed: " + e.getMessage());
        }
    }

    /**
     * ❤️ HEALTH CHECK
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {

        log.info("📥 API CALL: Health Check");

        return ResponseEntity.ok("NiFi Flow App is running 🚀");
    }
}