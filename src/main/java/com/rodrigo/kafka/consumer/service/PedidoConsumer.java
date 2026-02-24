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

    private static final int MAX_RETRIES = 3;

    @KafkaListener(topics = "pedido-topic", groupId = "pedido-group")
    public void consumir(ConsumerRecord<String, Message> record, Acknowledgment ack) {

        int tentativas = 0;
        boolean processado = false;

        while (!processado && tentativas < MAX_RETRIES) {
            tentativas++;

            try {
                log.info("infoMessage: Recebendo mensagem: {} | Tentativa: {}", record.value(), tentativas);

                if (record.value().getDescricao().toString().contains("erro")) {
                    throw new RuntimeException("Erro simulado");
                }

                log.info("infoMessage: Mensagem processada com sucesso: {}", record.value());

                ack.acknowledge();
                processado = true;

            } catch (Exception e) {
                log.error("errorMessage: Erro ao processar mensagem: {} | Tentativa: {} | Payload: {}",
                        e.getMessage(), tentativas, record.value());

                if (tentativas >= MAX_RETRIES) {
                    dlqProducer.enviarParaDlq(record.value());
                    log.error("errorMessage: Mensagem enviada para DLQ após {} tentativas: {}", tentativas, record.value());
                    ack.acknowledge();
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("errorMessage: Thread interrompida durante retry");
                    }
                }
            }
        }
    }
}