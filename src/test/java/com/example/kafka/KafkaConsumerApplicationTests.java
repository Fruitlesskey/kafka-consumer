package com.example.kafka;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.example.kafka.config.KafkaConsumerConfig;
import com.example.kafka.consumer.KafkaMessageListener;
import com.example.kafka.dto.Customer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(classes = {KafkaConsumerConfig.class, KafkaMessageListener.class})
@Testcontainers
@Slf4j
//@TestPropertySource(locations = "classpath:application.yml")
@ContextConfiguration(classes = {KafkaContainerTest.class, ConfigProducer.class, KafkaProducerCustom.class})
class KafkaConsumerApplicationTests {

  @Value("${name.topic}")
  private String nameTopic;

  @Autowired
  ConfigProducer configProducer;

  @Autowired
  private KafkaTemplate<String, Customer> kafkaTemplate;

  @Autowired
  private KafkaProducerCustom<String, Customer> kafkaProducerCustom;

  @SpyBean
  private KafkaMessageListener kafkaMessageListener;

  @Captor
  private ArgumentCaptor<Customer> captor;
//  @Container
//  static KafkaContainer kafka = new KafkaContainer(
//      DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

//  @DynamicPropertySource
//  public static void initKafkaProperties(DynamicPropertyRegistry registry) {
//    log.info("Init initKafkaProperties started...");
//    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
//    log.info("Init initKafkaProperties ended...");
//  }

  @Test
  void testConsume() {
//    log.info("testConsume execution started... {} +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++", configProducer.bootstrapAddress);
//    Customer customer = new Customer(111, "test user", "test@test", 1134144);
//    CompletableFuture<SendResult<String, Customer>> future = kafkaTemplate.send(nameTopic, customer);
//    log.error("Message was send to topic++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
//    future.thenAccept(result -> {
//      kafkaMessageListener.consume(captor.capture());
//      Assertions.assertEquals(customer.getId(), captor.getValue().getId());
//      log.error("{} ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++", captor.getValue());
//    });
    Customer personDto = Customer.builder().name("Albert").id(1).email("test@test").contactNo(1134144).build();

//    kafkaTemplate.executeInTransaction(kt -> {
//      kt.send(nameTopic, personDto);
//      log.error("+++++++++++++++++++++++++++++++++++Send message in kafka = {}", personDto);
//      return null;
//    });

    kafkaProducerCustom.send(nameTopic, personDto);
    verify(kafkaMessageListener, timeout(1000)).consume(captor.capture());

    Customer customer = captor.getValue();
    Assertions.assertAll(() -> {
//      Assertions.assertTrue();
      Assertions.assertEquals(personDto.getId(),
          customer.getId());
    });
    log.error("+++++++++++++++++++++++++++++++++++customDeserializerDto = {}", customer);
//    log.info("testConsume execution ended...");
//    await().pollInterval(Duration.ofSeconds(3))
//        .atMost(10, TimeUnit.SECONDS)
//        .untilAsserted(() ->{
//          //assert statement
//        });

  }

}
