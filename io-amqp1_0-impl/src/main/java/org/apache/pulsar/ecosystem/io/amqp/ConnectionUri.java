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

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Uri of connection.
 */
@Data
@Accessors(chain = true)
public class ConnectionUri {
    private String protocol;
    private String host;
    private int port;
    private List<String> urlOptions;

    public static ConnectionUri load(Map<String, Object> config) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(config), ConnectionUri.class);
    }

    public void validate() throws ConfigurationInvalidException {
        if (StringUtils.isEmpty(this.protocol)
                || StringUtils.isEmpty(this.host)
                || port <= 0){
            throw new ConfigurationInvalidException("Protocol, host and port should be set for all uris in "
                    + "connection");
        }
    }

    public String getUri() {
        return this.protocol + "://" + host + ":" + port + getUrlOptionsAsString();
    }

    private String getUrlOptionsAsString() {
        if (urlOptions != null && !urlOptions.isEmpty()){
            return "?" + String.join("&", urlOptions);
        }
        return "";
    }
}
