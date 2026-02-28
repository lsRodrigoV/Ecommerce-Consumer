package com.rodrigo.kafka.consumer.service;

import com.ecommerce.contracts.Message;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PedidoListener {

    private final PedidoConsumerService service;

    @KafkaListener(
            topics = "pedido-topic",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(ConsumerRecord<String, Message> record,
                         Acknowledgment ack) {

        service.receive(record, ack);
    }
}