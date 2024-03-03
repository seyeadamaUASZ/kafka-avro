package com.sid.gl.consumer;

import com.sid.gl.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "${topic.name}")
    public void read(ConsumerRecord<String, Employee> consumerRecord){
       String key = consumerRecord.key();
       Employee employee = consumerRecord.value();

       log.info("Avro message received key {}  value {} ",key,employee.toString());
    }
}
