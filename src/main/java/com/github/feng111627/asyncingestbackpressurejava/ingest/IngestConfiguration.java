package com.github.feng111627.asyncingestbackpressurejava.ingest;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfiguration {
}
