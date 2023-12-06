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

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;

/**
 * QpidJms connector base config.
 */
@Data
@Accessors(chain = true)
public class AmqpBaseConfig {

    @FieldDoc(
            sensitive = true,
            defaultValue = "",
            help = "the username of the AMQP."
    )
    private String username;

    @FieldDoc(
            sensitive = true,
            defaultValue = "",
            help = "the password of the AMQP."
    )
    private String password;
    @Deprecated
    /* Use Connection with failover support instead
     For single uri configuration without failover support provide a list with one ConnectionUri in Connection
    */
    private String host;
    @Deprecated
    /* Use Connection with failover support instead
     For single uri configuration without failover support provide a list with one ConnectionUri in Connection
    */
    private String protocol;
    @Deprecated
    /* Use Connection with failover support instead
      For single uri configuration without failover support provide a list with one ConnectionUri in Connection
     */
    private int port;

    @FieldDoc(
            defaultValue = "",
            help = "the AMQP queue."
    )
    private String queue;

    @FieldDoc(
            defaultValue = "",
            help = "the AMQP topic."
    )
    private String topic;

    @FieldDoc(
            defaultValue = "",
            help = "the connection url of AMQP."
    )
    public Connection connection;

    @FieldDoc(
            defaultValue = "false",
            help = "whether message is test format only."
    )
    private boolean onlyTextMessage = false;

    public static AmqpBaseConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(config), AmqpBaseConfig.class);
    }

    public void validate() throws ConfigurationInvalidException {

        if (connection != null) {
            if (isValidProtocolConfiguration() || isValidHostConfiguration() || isValidPortConfiguration()){
                throw new ConfigurationInvalidException(
                        "When connection is set, protocol, host, port should be empty.");
            }
            this.connection.validate();
        }

        if (connection == null && !isValidUrlComponentConfiguration()){
            throw new ConfigurationInvalidException(
                    "Protocol, host, port should be set when no connection is provided.");
        }

        if (StringUtils.isNotEmpty(queue) && StringUtils.isNotEmpty(topic)) {
            throw new ConfigurationInvalidException("The queue and topic couldn't be set at the same time.");
        }

        String destinationName = null;
        if (StringUtils.isNotEmpty(queue)) {
            destinationName = queue;
        } else if (StringUtils.isNotEmpty(topic)) {
            destinationName = topic;
        }
        if (StringUtils.isEmpty(destinationName)) {
            throw new ConfigurationInvalidException("The queue and topic all not set.");
        }
    }

    public String getUri() {
        if (connection != null) {
            return connection.getUri();
        }
        return this.protocol + "://" + host + ":" + port;
    }

    public JmsDestination getDestination() {
        JmsDestination destination = null;
        if (StringUtils.isNotEmpty(queue)) {
            destination = new JmsQueue(queue);
        } else if (StringUtils.isNotEmpty(topic)) {
            destination = new JmsTopic(topic);
        }
        return destination;
    }

    private boolean isValidUrlComponentConfiguration() {
        return isValidProtocolConfiguration()
                && isValidHostConfiguration()
                && isValidPortConfiguration();
    }

    private boolean isValidProtocolConfiguration() {
        return StringUtils.isNotEmpty(protocol);
    }

    private boolean isValidHostConfiguration() {
        return StringUtils.isNotEmpty(host);
    }

    private boolean isValidPortConfiguration() {
        return port > 0;
    }



}
