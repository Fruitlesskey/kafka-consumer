package com.example.kafka.consumer;

import com.example.kafka.dto.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaMessageListener {

  @KafkaListener(
      topics = "${name.topic}",
      groupId = "Mygroup",
      containerFactory = "kafkaListenerContainerFactory"
  )
  public void consume(Customer customer) {
    log.info("consumer-1 consume the events {}", customer);

  }

  @KafkaListener(
      topics = "MasterTopic1",
      groupId = "MasterGroup",
      topicPartitions = {@TopicPartition(topic = "MasterTopic1", partitions = {"3"})},
      containerFactory = "stringKafkaListenerContainerFactory"
  )
  public void consumeMessage(String str) {
    log.info("consumer-1 consume the events {}", str);

  }

}
