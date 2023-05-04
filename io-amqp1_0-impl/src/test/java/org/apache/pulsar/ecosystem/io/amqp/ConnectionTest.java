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

import org.junit.Assert;
import org.junit.Test;

/**
 * ConnectionTest.
 */
public class ConnectionTest {

    @Test
    public void validateTestConnection() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> paramsMap = getConnectionConfig(uris);
        Connection connectionConfig = Connection.load(paramsMap);
        connectionConfig.validate();

        Assert.assertEquals("amqp", connectionConfig.getUris().get(0).getProtocol());
        Assert.assertEquals("localhost", connectionConfig.getUris().get(0).getHost());
        Assert.assertEquals(5672, connectionConfig.getUris().get(0).getPort());
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true", connectionConfig.getUri());
    }

    @Test
    public void validateTestNullConnectionUri() throws Exception {

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(null);

        Map<String, Object> paramsMap = getConnectionConfig(uris);

        Connection connectionConfig = Connection.load(paramsMap);

        try {
            connectionConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("uris in Connection should contain a valid "
                    + "combination of protocol, host and port", e.getMessage());
        }
    }

    @Test
    public void validateTestInvalidHostInConnectionUri() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> paramsMap = getConnectionConfig(uris);

        Connection connectionConfig = Connection.load(paramsMap);

        try {
            connectionConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for all uris in connection",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestInvalidProtocolInConnectionUri() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> paramsMap = getConnectionConfig(uris);

        Connection connectionConfig = Connection.load(paramsMap);

        try {
            connectionConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for all uris in connection",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestInvalidPortInConnectionUri() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 0, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> paramsMap = getConnectionConfig(uris);

        Connection connectionConfig = Connection.load(paramsMap);

        try {
            connectionConfig.validate();
            Assert.fail("The test should fail because connection contains uris with null value");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for all uris in connection",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestConflictMultipleUrisAndNoFailover() throws Exception {
        Map<String, Object> uri1 =
                getConnectionUriConfig("localhost", 5672, "amqp",
                        Arrays.asList("transport.tcpKeepAlive=true"));
        Map<String, Object> uri2 =
                getConnectionUriConfig("localhost", 5672, "amqp",
                        Arrays.asList("transport.tcpKeepAlive=true"));

        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(uri1);
        uris.add(uri2);

        Map<String, Object> paramsMap = getConnectionConfig(uris);
        Connection connectionConfig = Connection.load(paramsMap);

        try {
            connectionConfig.validate();
            Assert.fail("The test should fail because multiple uris on connection is currently only "
                    + "supported when useFailover is true");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Multiple uri's currently only supported when failover is configured "
                            + "with useFailover set to true",
                    e.getMessage());
        }
    }
    @Test
    public void validateTestEmptyUris() throws Exception {

        ArrayList<Map<String, Object>> uris = new ArrayList<>();

        Map<String, Object> paramsMap = getConnectionConfig(uris);
        Connection connectionConfig = Connection.load(paramsMap);

        try {
            connectionConfig.validate();
            Assert.fail("The test should fail because uris is empty in connection");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("No uri's specified in connection", e.getMessage());
        }
    }

    @Test
    public void validateTestUriBasedOnConnection() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true")));

        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getConnectionConfig(uris, failover);
        Connection connectionConfig = Connection.load(paramsMap);
        connectionConfig.validate();

        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true", connectionConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnConnectionWithMultipleUrlOptions() throws Exception {
        ArrayList<Map<String, Object>> uris = new ArrayList<>();
        uris.add(getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true", "extra_property=false")));
        Map<String, Object> failover = getFailoverConfig(false, "",
                Collections.emptyList());

        Map<String, Object> paramsMap = getConnectionConfig(uris, failover);
        Connection connectionConfig = Connection.load(paramsMap);
        connectionConfig.validate();
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true&extra_property=false",
                connectionConfig.getUri());
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

        Map<String, Object> paramsMap = getConnectionConfig(uris, failover);
        Connection connectionConfig = Connection.load(paramsMap);
        connectionConfig.validate();
        Assert.assertEquals("failover:(amqp://localhost:5672?transport.tcpKeepAlive=true,"
                + "amqp://localhost:5672?transport.tcpKeepAlive=true)?jms.clientID=foo&failover.maxReconnectAttempts=20"
                , connectionConfig.getUri());
    }

    private Map<String, Object> getConnectionConfig(ArrayList<Map<String, Object>> uris,
                                                        Map<String, Object> failover) {
        Map<String, Object> connectionMap = new HashMap<>();

        connectionMap.put("uris", uris);
        connectionMap.put("failover", failover);

        return connectionMap;
    }

    private Map<String, Object> getConnectionConfig(ArrayList<Map<String, Object>> uris) {
        Map<String, Object> connectionMap = new HashMap<>();

        connectionMap.put("uris", uris);

        return connectionMap;
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
