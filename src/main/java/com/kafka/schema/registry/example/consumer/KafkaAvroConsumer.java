package com.kafka.schema.registry.example.consumer;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.kafka.schema.registry.example.model.Employee;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaAvroConsumer {

    @KafkaListener(topics = "${topic.name}", containerFactory = "kafkaListenerContainerFactory")
    public void read(ConsumerRecord<String, Employee> consumerRecord, Acknowledgment acknowledgment) {
        try {
            String key = consumerRecord.key();
            Employee employee = consumerRecord.value();
            log.info("Message received - key: {}, id: {}, partition: {}, offset: {}",
                    key, employee.getId(), consumerRecord.partition(), consumerRecord.offset());

            // Process message here

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing message - partition: {}, offset: {}, error: {}",
                    consumerRecord.partition(), consumerRecord.offset(), e.getMessage(), e);
            acknowledgment.nack(Duration.ofSeconds(1));
        }
    }
}
