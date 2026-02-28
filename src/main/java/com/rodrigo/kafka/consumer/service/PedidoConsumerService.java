package com.rodrigo.kafka.consumer.service;

import com.ecommerce.contracts.Message;
import com.rodrigo.kafka.consumer.entity.PedidoEntity;
import com.rodrigo.kafka.consumer.producerdlq.DlqProducer;
import com.rodrigo.kafka.consumer.repository.PedidoRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidoConsumerService extends KafkaConsumerService<Message> {

    private final DlqProducer dlqProducer;
    private final PedidoRepository pedidoRepository;

    @Override
    @Transactional
    @CircuitBreaker(name = "pedidoService", fallbackMethod = "fallbackCircuit")
    protected void process(Message payload) {

        log.info("Processando pedido: {}", payload);

        if (payload.getDescricao() == null) {
            throw new RuntimeException("Descrição não pode ser nula");
        }

        if (payload.getDescricao().toString().contains("erro")) {
            throw new RuntimeException("Erro simulado");
        }

        PedidoEntity entity = PedidoEntity.builder()
                .descricao(payload.getDescricao().toString())
                .build();

        pedidoRepository.save(entity);

        log.info("Pedido salvo no PostgreSQL com sucesso.");
    }

    @Override
    protected void handleError(ConsumerRecord<String, Message> record, Exception e) {
        dlqProducer.enviarParaDlq(record.value());
    }

    public void fallbackCircuit(Message payload, Throwable t) {

        log.error("CircuitBreaker acionado. Enviando para DLQ: {}", payload, t);

        dlqProducer.enviarParaDlq(payload);
    }
}