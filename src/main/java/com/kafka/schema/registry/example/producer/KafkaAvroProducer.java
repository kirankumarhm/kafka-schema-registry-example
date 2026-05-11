package com.kafka.schema.registry.example.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.kafka.schema.registry.example.model.Employee;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaAvroProducer {

	@Value("${topic.name}")
	private String topicName;

	private final KafkaTemplate<String, Employee> kafkaTemplate;

	public CompletableFuture<SendResult<String, Employee>> send(Employee employee) {
		String key = String.valueOf(employee.getId());
		CompletableFuture<SendResult<String, Employee>> future = kafkaTemplate.send(topicName, key, employee);

		future.whenComplete((result, ex) -> {
			if (ex == null) {
				log.info("Message sent - key: {}, offset: {}, partition: {}",
						key, result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
			} else {
				log.error("Failed to send message - key: {}, error: {}", key, ex.getMessage(), ex);
			}
		});

		return future;
	}
}
