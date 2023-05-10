package com.wheelseye.cyborg.service.kafka;

// bajrang.davda@meticuousorbit.com 

// Bali@@Bali00
// desktop 65.0.203.153:3389
// desktop 65.0.203.153:3389
// root kali
// 10.20.5.194
// 10.20.4.112
// 10.20.4.45
// 10.20.4.171

import com.wheelseye.cyborg.dto.CyborgFTagEscalationDTO;
import com.wheelseye.cyborg.dto.KafkaChargeBackTransactionDTO;
import com.wheelseye.cyborg.enums.EscalationAction;
import com.wheelseye.cyborg.enums.FastagEscalationTypeEnum;
import com.wheelseye.cyborg.service.EscalationService;
import com.wheelseye.cyborg.util.TransformUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class KafkaTransactionConsumerService {

    @Autowired
    private EscalationService escalationService;

    @KafkaListener(topics = "${kafka.consumer.escalation.topic}", containerFactory = "kafkaChargebackBatchedListenerContainerFactory")
    public void receiveMessage(@Payload List<String> kafkaMessages) {
        MDC.put("requestId", "fastag-escalation-consumer-" + System.nanoTime());
        LOG.info("Received Fastag Escalation : {}", kafkaMessages);
        parseAndProcessEscalations(kafkaMessages);
    }

    private void parseAndProcessEscalations(List<String> kafkaMessages) {
        List<CyborgFTagEscalationDTO> escalationDTOS = kafkaMessages.stream()
                .map(k -> TransformUtil.fromJson(k, CyborgFTagEscalationDTO.class))
                .collect(Collectors.toList());
        for (CyborgFTagEscalationDTO dto : escalationDTOS) {
            try {
                if (EscalationAction.CREATE.equals(dto.getAction())) {
                    escalationService.createFastagAutoEscalation(dto);
                } else if (EscalationAction.UPDATE.equals(dto.getAction())) {
                    if (FastagEscalationTypeEnum.REPLACE_TAG_CLASS.equals(dto.getEscalationType())) {
                        escalationService.updateFastagTagReplacementEscalation(dto);
                    } else if (FastagEscalationTypeEnum.getFastagTxnEscalations().contains(dto.getEscalationType())) {
                        escalationService.updateFastagTransactionEscalation(dto);
                    }
                }
            } catch (Exception e) {
                LOG.error("Exception processing escalation: {}, DTO:: {}", e.getStackTrace(), dto);
            }
        }
    }

    @KafkaListener(topics = "${kafka.chargebackTxn.topic}", containerFactory = "kafkaChargebackBatchedListenerContainerFactory")
    public void receiveChargeBackMessage(@Payload List<String> kafkaMessages) {
        MDC.put("requestId", "fastag-chargeback-consumer-" + System.nanoTime());
        LOG.info("Received ChargeBack transactions : {}", kafkaMessages);
        parseAndProcessChargebacks(kafkaMessages);
    }

    private void parseAndProcessChargebacks(List<String> kafkaMessages) {
        List<KafkaChargeBackTransactionDTO> chargeBackTransactionDTOS = kafkaMessages.stream()
                .map(k -> TransformUtil.fromJson(k, KafkaChargeBackTransactionDTO.class))
                .collect(Collectors.toList());
        for (KafkaChargeBackTransactionDTO dto : chargeBackTransactionDTOS) {
            try {
                escalationService.processFastagChargeBack(dto);
            } catch (Exception e) {
                e.fillInStackTrace();
                LOG.error("Exception while processFastagChargeBack ::{}, dto :: {}", e.getMessage(), dto);
            }
        }
    }
}