package com.rodrigo.kafka.consumer.service;

import com.ecommerce.contracts.Message;
import com.rodrigo.kafka.consumer.producerdlq.DlqProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidoConsumer {

    private final DlqProducer dlqProducer;

    private int contador = 0;

    @KafkaListener(topics = "pedido-topic", groupId = "pedido-group")
    public void consumir(
            ConsumerRecord<String, Message> record,
            Acknowledgment ack) {

        contador++;

        try {
            log.info("Recebendo mensagem: {}", record.value());

            // 💥 Simula erro a cada segunda mensagem
            if (contador % 2 == 0) {
                throw new RuntimeException("Erro simulado");
            }

            // Processamento normal
            log.info("Processado com sucesso!");

            ack.acknowledge();

        } catch (Exception e) {
            log.error("Erro ao processar mensagem: {}", e.getMessage());

            // Envia para DLQ
            dlqProducer.enviarParaDlq(record.value());

            // Confirma offset para não travar fila
            ack.acknowledge();
        }
    }
}