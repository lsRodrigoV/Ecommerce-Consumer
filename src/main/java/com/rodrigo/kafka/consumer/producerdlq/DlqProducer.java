package com.rodrigo.kafka.consumer.producerdlq;


import com.ecommerce.contracts.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DlqProducer {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public void enviarParaDlq(Message message) {
        kafkaTemplate.send("pedido-topic.DLQ", message);
    }
}