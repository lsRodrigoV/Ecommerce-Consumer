package com.rodrigo.kafka.kafka_demo.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class PedidoConsumer {

    @KafkaListener(
            topics = "pedido-topic",
            groupId = "pedido-group"
    )
    public void consumir(ConsumerRecord<String, String> record) {

        String key = record.key();
        String value = record.value();

        System.out.println("Mensagem recebida:");
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
    }
}