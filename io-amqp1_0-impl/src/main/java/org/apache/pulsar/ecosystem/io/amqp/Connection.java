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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Connection with a list of URI's and whether the failover configuration.
 */
@Data
@Accessors(chain = true)
public class Connection {

    private Failover failover;
    private List<ConnectionUri> uris;

    public static Connection load(Map<String, Object> config) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(config), Connection.class);
    }

    public String getUri() {
        String prefix = failover != null ? failover.getFailoverPrefix() : "";
        String suffix = failover != null ? failover.getFailoverSuffix() : "";

        return this.uris
                .stream()
                .map(ConnectionUri::getUri)
                .collect(Collectors.joining(",", prefix, suffix));
    }

    public void validate() throws ConfigurationInvalidException {

        if (null == uris || uris.isEmpty()) {
            throw new ConfigurationInvalidException("No uri's specified in connection");
        }

        if (uris.size() > 1 && (failover == null || !failover.isUseFailover())){
            throw new ConfigurationInvalidException("Multiple uri's currently only supported when failover is "
                    + "configured with useFailover set to true");
        }

        if (failover != null) {
            failover.validate();
        }

        for (ConnectionUri connectionUri : uris) {
            if (connectionUri == null) {
                throw new ConfigurationInvalidException("uris in Connection should contain a valid "
                        + "combination of protocol, host and port");
            }
            connectionUri.validate();
        }
    }
}
