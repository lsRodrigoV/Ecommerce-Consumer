package com.rodrigo.kafka.consumer.service;

import com.ecommerce.contracts.Message;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
public abstract class KafkaConsumerService<T> {

    public void receive(ConsumerRecord<String, T> record,
                        Acknowledgment ack) {

        try {

            infoMessage(record);

            process(record.value());

            ack.acknowledge();

        } catch (Exception e) {

            errorMessage(record, e);

            handleError(record, e);

            ack.acknowledge();
        }
    }

    protected abstract void process(T payload);

    protected abstract void handleError(ConsumerRecord<String, T> record, Exception e);

    protected void infoMessage(ConsumerRecord<String, T> record) {
        log.info("Evento recebido: {}", record.value());
    }

    protected void errorMessage(ConsumerRecord<String, T> record, Exception e) {
        log.error("Erro ao processar evento {}", record.value(), e);
    }
}