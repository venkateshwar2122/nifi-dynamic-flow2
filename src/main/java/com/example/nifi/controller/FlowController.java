package com.example.nifi.controller;

import com.example.nifi.dto.FlowRequest;
import com.example.nifi.service.FlowBuilderService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/flow")
public class FlowController {

    private final FlowBuilderService service;

    public FlowController(FlowBuilderService service) {
        this.service = service;
    }

    @PostMapping("/create")
    public String create(@RequestBody FlowRequest req) {
        return service.buildFlow(req.getName());
    }
}