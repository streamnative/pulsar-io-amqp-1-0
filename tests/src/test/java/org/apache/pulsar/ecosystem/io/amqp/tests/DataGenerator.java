package org.apache.pulsar.ecosystem.io.amqp.tests;

import lombok.Cleanup;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsSession;
import org.junit.Test;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;

public class DataGenerator {

    @Test
    public void produceData() throws Exception {

        JmsConnectionFactory factory = new JmsConnectionFactory("default", "default", "amqp://localhost:5672");
        JMSContext jmsContext = factory.createContext();

        Destination destination = new JmsQueue("user-op-queue");

        JmsSession jmsSession = (JmsSession) factory.createConnection().createSession();
        JMSProducer jmsProducer = jmsContext.createProducer();
        ConnectionFactory jmsConnectionFactory = new JmsConnectionFactory("guest", "guest", "amqp://localhost:5672");
//        @Cleanup
//        Connection connection = jmsConnectionFactory.createConnection("guest", "guest");
//        connection.start();

        JMSProducer producer = jmsContext.createProducer();

        for (int i = 0; i < 10; i++) {
            jmsProducer.send(destination, "text str - " + i);
        }
        System.out.println("finish to produce data");
    }

    @Test
    public void receiveData() throws Exception {
        ConnectionFactory jmsConnectionFactory = new JmsConnectionFactory("amqp://localhost:5672");
        @Cleanup
        Connection connection = jmsConnectionFactory.createConnection("default", "default");
        connection.start();

        @Cleanup
        JMSContext jmsContext = jmsConnectionFactory.createContext();

        Destination destination = new JmsQueue("user-op-queue-pulsar");
        JMSConsumer consumer = jmsContext.createConsumer(destination);

        while (true) {
            Message message = consumer.receive();
            System.out.println("receive msg " + ((TextMessage) message).getText());
        }
    }

}
