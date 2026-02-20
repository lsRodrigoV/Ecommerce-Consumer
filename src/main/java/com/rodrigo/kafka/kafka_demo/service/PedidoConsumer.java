package com.rodrigo.kafka.kafka_demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class PedidoConsumer {

    @KafkaListener(
            topics = "pedido-topic",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(ConsumerRecord<String, String> record,
                         Acknowledgment ack) {

        log.info("Mensagem recebida");
        log.info("Partition: {}", record.partition());
        log.info("Offset: {}", record.offset());
        log.info("Key: {}", record.key());
        log.info("Value: {}", record.value());

        // Simula processamento
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Confirma processamento
        ack.acknowledge();
    }
}
