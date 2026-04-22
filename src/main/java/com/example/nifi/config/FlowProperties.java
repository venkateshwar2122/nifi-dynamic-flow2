package com.example.nifi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "flow")
public class FlowProperties {

    public Scheduling scheduling;
    public Dbcp dbcp;
    public Mongo mongo;
    public Query query;

    public static class Scheduling {
        public String query;
    }

    public static class Dbcp {
        public String driverClass;
        public String driverLocation;
        public String url;
        public String user;
        public String password;
    }

    public static class Mongo {
        public String uri;
        public String user;
        public String password;
        public String db;
        public String collection;
    }

    public static class Query {
        public String table;
        public String pk;
    }
}