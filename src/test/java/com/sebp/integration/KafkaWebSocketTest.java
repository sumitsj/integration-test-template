package com.sebp.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class KafkaWebSocketTest {

    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    private static KafkaProducer<String, String> producer;

    @BeforeClass
    public void setUp() {
        kafkaContainer.start();

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());

        producer = new KafkaProducer<>(producerProperties);
    }

    @AfterClass
    public void tearDown() {
        producer.close();
        kafkaContainer.stop();
    }

    @Test
    public void testKafkaToWebSocketFlow() throws Exception {
        String topic = "test-topic";
        String testMessage = "Hello, Kafka!";

        // WebSocket client setup
        BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        StandardWebSocketClient webSocketClient = new StandardWebSocketClient();
        webSocketClient.doHandshake(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(org.springframework.web.socket.WebSocketSession session, org.springframework.web.socket.TextMessage message) throws Exception {
                messages.offer(message.getPayload());
            }
        }, null, "ws://localhost:8080/websocket-endpoint");

        // Produce a message to Kafka
        producer.send(new ProducerRecord<>(topic, "key", testMessage));

        // Wait and verify the message is received on WebSocket
        String receivedMessage = messages.poll(5, TimeUnit.SECONDS);
        assertEquals(receivedMessage, testMessage, "WebSocket did not receive the expected message.");
    }
}
