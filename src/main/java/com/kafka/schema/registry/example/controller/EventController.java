package com.kafka.schema.registry.example.controller;

import java.util.concurrent.CompletableFuture;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.schema.registry.example.model.Employee;
import com.kafka.schema.registry.example.producer.KafkaAvroProducer;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Events", description = "Kafka Employee Event Publishing API")
public class EventController {

	private final KafkaAvroProducer producer;

	@Operation(summary = "Publish Employee event", description = "Serializes Employee as Avro and publishes to Kafka topic")
	@ApiResponses({
			@ApiResponse(responseCode = "202", description = "Message accepted and published to Kafka"),
			@ApiResponse(responseCode = "500", description = "Failed to publish message")
	})
	@PostMapping("/events")
	public CompletableFuture<ResponseEntity<String>> sendMessage(@RequestBody Employee employee) {
		return producer.send(employee)
				.thenApply(result -> ResponseEntity.status(HttpStatus.ACCEPTED)
						.body("Message published - offset: " + result.getRecordMetadata().offset()))
				.exceptionally(ex -> ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
						.body("Failed to publish message: " + ex.getMessage()));
	}
}
