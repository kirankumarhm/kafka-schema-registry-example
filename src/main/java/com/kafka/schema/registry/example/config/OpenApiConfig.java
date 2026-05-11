package com.kafka.schema.registry.example.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Kafka Schema Registry Example API")
                        .version("1.0.0")
                        .description("REST API for publishing Avro-serialized Employee events to Kafka with Schema Registry")
                        .contact(new Contact().name("Dev Team")));
    }
}
