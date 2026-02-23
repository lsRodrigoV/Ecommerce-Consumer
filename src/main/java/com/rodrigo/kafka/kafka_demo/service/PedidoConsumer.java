package com.rodrigo.kafka.kafka_demo.service;

import com.ecommerce.contracts.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PedidoConsumer {

    @KafkaListener(
            topics = "pedido-topic",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(ConsumerRecord<String, Message> record,
                         Acknowledgment ack) {

        GenericRecord message = record.value();

        log.info("========= MENSAGEM RECEBIDA =========");
        log.info("Partition: {}", record.partition());
        log.info("Offset: {}", record.offset());
        log.info("Key: {}", record.key());

        if (message != null) {
            log.info("ID: {}", message.get("id"));
            log.info("Timestamp: {}", message.get("timestamp"));
            log.info("Descrição: {}", message.get("descricao"));
        }

        try {
            Thread.sleep(500); // Simula processamento
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Confirma manualmente o offset
        ack.acknowledge();

        log.info("Offset confirmado com sucesso!");
        log.info("======================================");
    }
}