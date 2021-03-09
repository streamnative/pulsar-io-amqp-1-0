/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.amqp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.provider.amqp.message.AmqpCodec;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.message.AmqpUtils;
import org.apache.qpid.proton.codec.ReadableBuffer;


/**
 * QpidJms sink connector.
 */
@Slf4j
public class AmqpSink implements Sink<byte[]> {

    private AmqpSinkConfig config;
    private JMSContext jmsContext;
    private JMSProducer jmsProducer;
    private JmsDestination destination;
    private AmqpConsumer amqpConsumer;

    @Override
    public void open(Map map, SinkContext sinkContext) throws Exception {
        config = AmqpSinkConfig.load(map);
        config.validate();

        JmsConnectionFactory factory;
        if (config.getUsername() != null && config.getPassword() != null) {
            factory = new JmsConnectionFactory(
                    config.getUsername(), config.getPassword(), config.getUri());
        } else {
            factory = new JmsConnectionFactory(config.getUri());
        }
        jmsContext = factory.createContext();

        destination = config.getDestination();
        if (destination == null) {
            throw new ConfigurationInvalidException("The destination is null.");
        }

        JmsSession jmsSession = (JmsSession) factory.createConnection().createSession();
        jmsProducer = jmsContext.createProducer();
        amqpConsumer = AmqpUtils.generateAmqpConsumer(
                ((AmqpJmsMessageFacade) ((JmsMessage) jmsSession.createMessage()).getFacade()), destination);
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        try {
            if (config.isOnlyTextMessage()) {
                TextMessage textMessage = jmsContext.createTextMessage(
                        new String(record.getValue(), StandardCharsets.UTF_8));
                jmsProducer.send(destination, textMessage);
            } else {
                AmqpJmsMessageFacade facade = AmqpCodec.decodeMessage(
                        amqpConsumer, new ReadableBuffer.ByteBufferReader(ByteBuffer.wrap(record.getValue())));
                jmsProducer.send(destination, facade.asJmsMessage());
            }
            record.ack();
        } catch (Exception e) {
            log.error("Failed to send message.", e);
            record.fail();
        }

//        jmsProducer.send(destination, textMessage).setAsync(new CompletionListener() {
//            @Override
//            public void onCompletion(Message message) {
//                record.ack();
//            }
//
//            @Override
//            public void onException(Message message, Exception exception) {
//                log.error("Failed send message to qpid jms broker.", exception);
//                record.fail();
//            }
//        });
    }

    @Override
    public void close() throws Exception {
        this.jmsContext.close();
    }
}
