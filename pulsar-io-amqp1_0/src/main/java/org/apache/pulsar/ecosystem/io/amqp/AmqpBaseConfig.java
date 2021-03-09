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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;


/**
 * QpidJms connector base config.
 */
@Data
@Accessors(chain = true)
public class AmqpBaseConfig {

    private String username;
    private String password;
    private String protocol;
    private String host;
    private int port;

    private String queue;
    private String topic;
    private boolean onlyTextMessage = false;

    public static AmqpBaseConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(config), AmqpBaseConfig.class);
    }

    public void validate() throws ConfigurationInvalidException {

        Objects.requireNonNull(protocol, "The protocol property not set.");
        Objects.requireNonNull(host, "The host property not set.");
        if (port <= 0) {
            throw new ConfigurationInvalidException("The configuration port is invalid.");
        }

        String destinationName = null;
        if (queue != null &&  !queue.isEmpty()) {
            destinationName = queue;
        } else if (topic != null && !topic.isEmpty()) {
            destinationName = topic;
        }
        Objects.requireNonNull(destinationName, "The queueName and topicName all not set.");
    }

    public String getUri() {
        return this.protocol + "://" + host + ":" + port;
    }

    public JmsDestination getDestination() {
        JmsDestination destination = null;
        if (queue != null && !queue.isEmpty()) {
            destination = new JmsQueue(queue);
        } else if (topic != null && !topic.isEmpty()) {
            destination = new JmsTopic(topic);
        }
        return destination;
    }

}
