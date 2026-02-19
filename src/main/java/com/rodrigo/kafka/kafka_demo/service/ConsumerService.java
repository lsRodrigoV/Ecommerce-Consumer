package com.rodrigo.kafka.kafka_demo.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics = "pedido-topic", groupId = "pedido-group")
    public void consumir(String mensagem) {
        System.out.println("Pedido recebido: " + mensagem);
    }
}

