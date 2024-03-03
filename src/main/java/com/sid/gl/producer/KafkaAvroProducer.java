package com.sid.gl.producer;

import com.sid.gl.dto.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaAvroProducer {
    @Value("${topic.name}")
    private String topicName;
    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    public void send(Employee employee){
        try{
            CompletableFuture<SendResult<String,Employee>> future=  kafkaTemplate.send(topicName,UUID.randomUUID().toString(),employee);
            future.whenComplete((result,ex)->{
                if(ex==null){
                    System.out.println("Sent message=[ "+employee.toString()+ "] with offset= ["+result.getRecordMetadata().offset() +"]");
                }else{
                    System.out.println(" Unable send message=["+employee.toString()+" ] due to:  "+ex.getMessage());
                }
            });
        }catch(Exception e){
            System.out.println("Error "+ e.getMessage());
        }
    }
}
