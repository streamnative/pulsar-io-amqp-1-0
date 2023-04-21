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

import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Connection with a list of URI's and whether the failover prefix should be used.
 */
@Data
@Accessors(chain = true)
public class Connection {

    private boolean useFailover;
    private List<ConnectionUri> uris;

    public String getUri(){
        String urisString = this.uris
                .stream()
                .map(ConnectionUri::getUri)
                .collect(Collectors.joining(","));
        return useFailover ? "failover:(" + urisString + ")" : urisString;
    }

    public void validate() throws ConfigurationInvalidException {

        if (null == uris || uris.isEmpty()){
            throw new ConfigurationInvalidException("No uri's specified in connection");
        }

        if (uris.size() > 1 && !useFailover) {
            throw new ConfigurationInvalidException("Multiple uri's currently only supported when useFailover is "
                    + "set to true");
        }

        for (ConnectionUri host : uris) {
            if (host == null){
                throw new ConfigurationInvalidException("Host cannot be null in connection");
            }
            host.validate();
        }
    }
}
