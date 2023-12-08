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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.io.core.SourceContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Amqp source config test.
 */
public class AmqpBaseConfigTest {


    @Test
    public void validateTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);
        baseConfig.validate();

        Assert.assertEquals(baseConfig.getProtocol(), "amqp");
        Assert.assertEquals(baseConfig.getHost(), "localhost");
        Assert.assertEquals(baseConfig.getPort(), 5672);
        Assert.assertEquals(baseConfig.getQueue(), "test-queue");
        Assert.assertEquals(baseConfig.getUri(), "amqp://localhost:5672");
    }

    @Test
    public void validateTestConnection() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));
        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);
        baseConfig.validate();

        Assert.assertEquals("amqp", baseConfig.getConnection().getUris().get(0).getProtocol());
        Assert.assertEquals("localhost", baseConfig.getConnection().getUris().get(0).getHost());
        Assert.assertEquals(5672, baseConfig.getConnection().getUris().get(0).getPort());
        Assert.assertEquals("test-queue", baseConfig.getQueue());
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true", baseConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnUrlComponents() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        baseConfig.validate();
        Assert.assertEquals("amqp://localhost:5672", baseConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnConnection() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        baseConfig.validate();
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true", baseConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnConnectionFailOnEmptyHost() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        try {
            baseConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for all uris in connection",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestUriBasedOnConnectionFailOnEmptyProtocol() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        try {
            baseConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for all uris in connection",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestUriBasedOnConnectionFailOnInvalidPort() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 0, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        try {
            baseConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for all uris in connection",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestFailoverUrl() throws Exception {
        Map<String, Object> uri1 =
                getConnectionUriConfig("localhost", 5672, "amqp",
                        Arrays.asList("transport.tcpKeepAlive=true"));
        Map<String, Object> uri2 =
                getConnectionUriConfig("localhost", 5672, "amqp",
                        Arrays.asList("transport.tcpKeepAlive=true"));

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(uri1);
        uris.add(uri2);
        Map<String, Object> failover = getFailoverConfig(true, "foo",
                Arrays.asList("failover.maxReconnectAttempts=20"));

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        baseConfig.validate();
        Assert.assertEquals("failover:(amqp://localhost:5672?transport.tcpKeepAlive=true,"
                + "amqp://localhost:5672?transport.tcpKeepAlive=true)?jms.clientID=foo&failover.maxReconnectAttempts=20"
                , baseConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnConnectionFailOnFailoverOptionsButNoJmsClientId() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(true, "",
                Arrays.asList("failover.maxReconnectAttempts=20"));

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        try {
            baseConfig.validate();
            Assert.fail("The test should fail because jmsClientId should be provided when "
                    + "failoverConfigurationOptions are provided");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("failoverConfigurationOptions is only supported when failover is configured with "
                            + "useFailover set to true and jmsClientId provided", e.getMessage());
        }
    }

    @Test
    public void validateTestUriBasedOnConnectionFailOnJmsClientIdButNoFailoverOptions() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(true, "foo",
                Collections.emptyList());

        Map<String, Object> paramsMap = getBaseConfigConnection(uris, failover);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        try {
            baseConfig.validate();
            Assert.fail("The test should fail because failoverConfigurationOptions should be provided when "
                    + "jmsClientId are provided");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("jmsClientId is only supported when failover is configured with "
                    + "useFailover set to true and failoverConfigurationOptions provided", e.getMessage());
        }
    }


    @Test
    public void uriConflictSetTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        paramsMap.putAll(getBaseConfigConnection(uris, failover));
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because connection is set at the same time as "
                    + "protocol, host, port and/or urlOptions");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("When connection is set, protocol, host, port should be empty.",
                    e.getMessage());
        }
    }

    @Test
    public void invalidConfigNoConnectionOrProtocolHostAndPortSet() throws Exception {
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because no connection, protocol, host and port are set");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host, port should be set when no connection is provided.",
                    e.getMessage());
        }
    }

    @Test
    public void destinationConflictSetTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        paramsMap.put("topic", "test-topic");
        SourceContext sourceContext = Mockito.mock(SourceContext.class);
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap, sourceContext);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because queue and topic all set at the same time.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("The queue and topic couldn't be set at the same time.", e.getMessage());
        }
    }

    @Test
    public void destinationNotSetTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        SourceContext sourceContext = Mockito.mock(SourceContext.class);
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap, sourceContext);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because queue and topic all not set.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("The queue and topic all not set.", e.getMessage());
        }
    }

    @Test
    public void destinationConfigTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        SourceContext sourceContext = Mockito.mock(SourceContext.class);
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap, sourceContext);
        sourceConfig.validate();

        paramsMap.remove("queue");
        paramsMap.put("topic", "test-topic");
        sourceConfig = AmqpSourceConfig.load(paramsMap, sourceContext);
        sourceConfig.validate();
    }

    private Map<String, Object> getBaseConfig() {
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("protocol", "amqp");
        paramsMap.put("host", "localhost");
        paramsMap.put("port", 5672);
        return paramsMap;
    }

    private Map<String, Object> getBaseConfigConnection(ArrayList<Map<String, Object>> uris,
                                                        Map<String, Object> failover) {
        Map<String, Object> paramsMap = new HashMap<>();
        Map<String, Object> connectionMap = new HashMap<>();

        connectionMap.put("uris", uris);
        connectionMap.put("failover", failover);

        paramsMap.put("connection", connectionMap);
        return paramsMap;
    }

    private Map<String, Object> getConnectionUriConfig(String host, int port, String protocol,
                                                       List<String> urlOptions) {
        Map<String, Object> hostMap = new HashMap<>();
        hostMap.put("protocol", protocol);
        hostMap.put("host", host);
        hostMap.put("port", port);
        hostMap.put("urlOptions", urlOptions);
        return hostMap;
    }

    private Map<String, Object> getFailoverConfig(boolean useFailover, String jmsClientId,
                                                  List<String> failoverConfigurationOptions) {
        Map<String, Object> failoverMap = new HashMap<>();
        failoverMap.put("useFailover", useFailover);
        failoverMap.put("jmsClientId", jmsClientId);
        failoverMap.put("failoverConfigurationOptions", failoverConfigurationOptions);
        return failoverMap;
    }
}
