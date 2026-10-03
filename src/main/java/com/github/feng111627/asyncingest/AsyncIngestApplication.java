package com.github.feng111627.asyncingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AsyncIngestApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncIngestApplication.class, args);
    }
}
