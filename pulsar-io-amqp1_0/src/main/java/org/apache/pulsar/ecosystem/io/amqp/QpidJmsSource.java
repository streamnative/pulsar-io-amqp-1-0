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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.message.JmsTextMessage;


/**
 * QpidJms source connector.
 */
@Slf4j
public class QpidJmsSource extends PushSource<byte[]> {

    private JMSContext jmsContext;
    private JMSConsumer jmsConsumer;

    @Override
    public void open(Map map, SourceContext sourceContext) throws Exception {
        QpidJmsSourceConfig config = QpidJmsSourceConfig.load(map);
        config.validate();

        JmsConnectionFactory factory;
        if (config.getUsername() != null && config.getPassword() != null) {
            factory = new JmsConnectionFactory(
                    config.getUsername(), config.getPassword(), config.getUri());
        } else {
            factory = new JmsConnectionFactory(config.getUri());
        }
        jmsContext = factory.createContext();

        Destination destination = config.getDestination();
        if (destination == null) {
            throw new ConfigurationInvalidException("The destination is null.");
        }

        jmsConsumer = jmsContext.createConsumer(destination);
        jmsConsumer.setMessageListener(new MessageListenerImpl(this));
    }

    /**
     * MessageListener implement.
     */
    @Data
    private static class MessageListenerImpl implements MessageListener {

        private final PushSource<byte[]> pushSource;

        @Override
        public void onMessage(Message message) {
            try {
                QpidJmsRecord record;
                if (message instanceof JmsTextMessage) {
                    record = new QpidJmsRecord(
                            Optional.empty(), ((JmsTextMessage) message).getText().getBytes(StandardCharsets.UTF_8));
                } else {
                    record = new QpidJmsRecord(Optional.empty(), new byte[0]);
                }
                pushSource.consume(record);
                record.completableFuture.whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to consume record.", throwable);
                        return;
                    }
                    try {
                        message.acknowledge();
                    } catch (JMSException e) {
                        log.error("Failed to ack qpid jms message.", e);
                    }
                });
            } catch (Exception e) {
                log.error("Failed to consume qpid jms message.", e);
            }
        }
    }

    /**
     * QpidJms record.
     */
    @Data
    private static class QpidJmsRecord implements Record<byte[]> {

        private final Optional<String> key;
        private final byte[] value;
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        @Override
        public void ack() {
            completableFuture.complete(null);
        }
    }

    @Override
    public void close() throws Exception {
        this.jmsConsumer.close();
        this.jmsContext.close();
    }

}
