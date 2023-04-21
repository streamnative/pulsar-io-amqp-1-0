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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


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

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
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
    public void validateTestConflictMultipleurisAndNoFailover() throws Exception {
        Map<String, Object> uri1 =
                getConnectionUriConfig("localhost", 5672, "amqp",
                        Arrays.asList("transport.tcpKeepAlive=true"));
        Map<String, Object> uri2 =
                getConnectionUriConfig("localhost", 5672, "amqp",
                        Arrays.asList("transport.tcpKeepAlive=true"));

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(uri1);
        uris.add(uri2);

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);

        try {
            baseConfig.validate();
            Assert.fail("The test should fail because multiple uris on connection is currently only "
                    + "supported when useFailover is true");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Multiple uri's currently only supported when useFailover is set to true",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestEmptyuris() throws Exception {

        ArrayList<Map<String, Object>> uris = new ArrayList<>();

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);

        try {
            baseConfig.validate();
            Assert.fail("The test should fail because uris is empty in connection");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("No uri's specified in connection", e.getMessage());
        }
    }

    @Test
    public void validateTestNullHost() throws Exception {

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(null);

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);

        try {
            baseConfig.validate();
            Assert.fail("The test should fail because no connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Host cannot be null in connection", e.getMessage());
        }
    }

    @Test
    public void validateTestIncompleteHost() throws Exception {

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig(null, 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);

        try {
            baseConfig.validate();
            Assert.fail("The test should fail because connection are incomplete");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for "
                    + "all uris in connection", e.getMessage());
        }
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

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        baseConfig.validate();
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true", baseConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnConnectionWithMultipleUrlOptions() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true", "extra_property=false")));

        Map<String, Object> paramsMap = getBaseConfigConnection(false, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        baseConfig.validate();
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true&extra_property=false",
                baseConfig.getUri());
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

        Map<String, Object> paramsMap = getBaseConfigConnection(true, uris);
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpSourceConfig.load(paramsMap);
        baseConfig.validate();
        Assert.assertEquals("failover:(amqp://localhost:5672?transport.tcpKeepAlive=true,"
                + "amqp://localhost:5672?transport.tcpKeepAlive=true)", baseConfig.getUri());
    }


    @Test
    public void uriConflictSetTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        paramsMap.putAll(getBaseConfigConnection(false, uris));
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because connectiondetails is set at the same time as "
                    + "protocol, host, port and/or urlOptions");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("When connection is set, protocol, host, port should be empty.",
                    e.getMessage());
        }
    }

    @Test
    public void invalidConfigNoRawUrlProtocolHostAndPort() throws Exception {
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
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
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
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
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
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        sourceConfig.validate();

        paramsMap.remove("queue");
        paramsMap.put("topic", "test-topic");
        sourceConfig = AmqpSourceConfig.load(paramsMap);
        sourceConfig.validate();
    }

    private Map<String, Object> getBaseConfig() {
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("protocol", "amqp");
        paramsMap.put("host", "localhost");
        paramsMap.put("port", 5672);
        return paramsMap;
    }

    private Map<String, Object> getBaseConfigConnection(boolean useFailover,
                                                               ArrayList<Map<String, Object>> uris) {
        Map<String, Object> paramsMap = new HashMap<>();
        Map<String, Object> connectionMap = new HashMap<>();

        connectionMap.put("useFailover", useFailover);
        connectionMap.put("uris", uris);

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
}
