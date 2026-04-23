package com.example.nifi.dto;

public class FlowRequest {

    private String name;

    // Default constructor (REQUIRED)
    public FlowRequest() {
    }

    public FlowRequest(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}