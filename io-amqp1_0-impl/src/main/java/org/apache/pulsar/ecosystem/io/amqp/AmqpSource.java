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

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageSupport;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.provider.amqp.message.AmqpCodec;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;


/**
 * QpidJms source connector.
 */
@Slf4j
public class AmqpSource extends PushSource<ByteBuffer> {

    private AmqpSourceConfig config;
    private JMSContext jmsContext;
    private JMSConsumer jmsConsumer;

    @Override
    public void open(Map map, SourceContext sourceContext) throws Exception {
        config = AmqpSourceConfig.load(map);
        config.validate();

        JmsConnectionFactory factory;
        if (config.getUsername() != null && config.getPassword() != null) {
            factory = new JmsConnectionFactory(
                    config.getUsername(), config.getPassword(), config.getUri());
        } else {
            factory = new JmsConnectionFactory(config.getUri());
        }
        log.info("config.getUri(): " + config.getUri());
        log.info("factory.getRemoteURI(): " + factory.getRemoteURI());
        log.info("config.getSessionMode(): " + config.getSessionMode());
        jmsContext = factory.createContext(config.getSessionMode());

        Destination destination = config.getDestination();
        if (destination == null) {
            throw new ConfigurationInvalidException("The destination is null.");
        }

        jmsConsumer = jmsContext.createConsumer(destination);
        jmsConsumer.setMessageListener(new MessageListenerImpl(this, config));
    }

    /**
     * MessageListener implement.
     */
    @AllArgsConstructor
    private static class MessageListenerImpl implements MessageListener {

        private final PushSource<ByteBuffer> pushSource;
        private final AmqpSourceConfig config;

        @Override
        public void onMessage(Message message) {
            try {
                QpidJmsRecord record;
                if (config.isOnlyTextMessage()) {
                    record = new QpidJmsRecord(
                            Optional.empty(),
                            ByteBuffer.wrap(((JmsTextMessage) message).getText().getBytes(StandardCharsets.UTF_8)),
                            message);
                } else {
                    ByteBuf byteBuf = AmqpCodec.encodeMessage(
                            (AmqpJmsMessageFacade) ((JmsMessage) message).getFacade());
                    record = new QpidJmsRecord(Optional.empty(), byteBuf.nioBuffer(), message);
                }
                pushSource.consume(record);
            } catch (Exception e) {
                log.error("Failed to consume qpid jms message.", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * QpidJms record.
     */
    @Data
    private static class QpidJmsRecord implements Record<ByteBuffer> {
        private final Optional<String> key;
        private final ByteBuffer value;
        private final Message jmsMessage;

        @Override
        public Schema<ByteBuffer> getSchema() {
            return Schema.BYTEBUFFER;
        }

        @Override
        public void ack() {
            try {
                jmsMessage.acknowledge();
            } catch (JMSException e) {
                log.error("Failed to acknowledge qpid jms message.", e);
            }
        }

        @Override
        public void fail() {
            try {
                if (jmsMessage instanceof JmsMessage) {
                    JmsMessage qpidJmsMessage = (JmsMessage) jmsMessage;
                    if (qpidJmsMessage.getAcknowledgeCallback() != null) {
                        qpidJmsMessage.getAcknowledgeCallback().setAckType(JmsMessageSupport.REJECTED);
                        qpidJmsMessage.acknowledge();
                    }
                }
            } catch (JMSException e) {
                log.error("Failed to reject qpid jms message.", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.jmsConsumer.close();
        this.jmsContext.close();
    }

}
