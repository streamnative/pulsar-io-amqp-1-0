---
description: The AMQP1_0 sink connector pulls messages from Pulsar topics and persist messages to AMQP broker.
author: ["ASF"]
contributors: ["ASF"]
language: Java
document: 
source: "https://github.com/streamnative/pulsar-io-activemq/tree/v2.5.1/src/main/java/org/apache/pulsar/ecosystem/io/activemq"
license: Apache License 2.0
tags: ["Pulsar IO", "AMQP", "Qpid", "JMS", "Sink"]
alias: AMQP1_0 Sink
features: ["Use AMQP1_0 sink connector to sync data from Pulsar"]
license_link: "https://www.apache.org/licenses/LICENSE-2.0"
icon: "https://www.amqp.org/sites/amqp.org/themes/genesis_amqp/images/amqp-logo.png"
download: "https://github.com/streamnative/pulsar-io-activemq/releases/download/v2.5.1/pulsar-io-activemq-2.5.1.nar"
support: StreamNative
support_link: https://streamnative.io
support_img: "/images/connectors/streamnative.png"
dockerfile: 
id: "io-amqp1_0-sink"
---

The AMQP1_0 sink connector pulls messages from Pulsar topics and persist messages to AMQP broker.

# Installation

```
git clone https://github.com/streamnative/pulsar-io-amqp-1-0.git
cd pulsar-io-amqp-1-0/
mvn clean install -DskipTests
cp pulsar-io-amqp1_0/target/pulsar-io-amqp1_0-0.0.1.nar $PULSAR_HOME/pulsar-io-amqp1_0-0.0.1.nar
```

# Configuration 

The configuration of the AMQP1_0 sink connector has the following properties.

## AMQP1_0 sink connector configuration

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `protocol` |String| true | "amqp" | The AMQP protocol. |
| `host` | String| true | " " (empty string) | The AMQP service host. |
| `port` | int |true | 5672 | The AMQP serrvice port. |
| `username` | String|false | " " (empty string) | The username used to authenticate to ActiveMQ. |
| `password` | String|false | " " (empty string) | The password used to authenticate to ActiveMQ. |
| `queue` | String|false | " " (empty string) | The queue name that messages should be read from or written to. |
| `topic` | String|false | " " (empty string) | The topic name that messages should be read from or written to. |
| `activeMessageType` | String|false |0 | The ActiveMQ message simple class name. |

## Configure AMQP1_0 sink connector

Before using the AMQP1_0 sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
        "tenant": "public",
        "namespace": "default",
        "name": "amqp1_0-sink",
        "inputs": ["user-op-queue-topic"],
        "archive": "connectors/pulsar-io-amqp1_0-{version}.nar",
        "parallelism": 1,
        "configs":
        {
            "protocol": "amqp",
            "host": "localhost",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "user-op-queue-pulsar"
        }
    }
    ```

* YAML

    ```yaml
    tenant: "public"
    namespace: "default"
    name: "amqp1_0-sink"
    inputs: 
      - "user-op-queue-topic"
    archive: "connectors/pulsar-io-amqp1_0-{version}.nar"
    parallelism: 1
    
    configs:
        protocol: "amqp"
        host: "localhost"
        port: "5672"
        username: "guest"
        password: "guest"
        queueName: "user-op-queue-pulsar"
    ```

# Usage

1. Prepare AMQP service, use the solace service.

    ```
    docker run -d -p 8080:8080 -p:8008:8008 -p:1883:1883 -p:8000:8000 -p:5672:5672 -p:9000:9000 -p:2222:2222 --shm-size=2g --env username_admin_globalaccesslevel=admin --env username_admin_password=admin --name=solace solace/solace-pubsub-standard
    ```

2. Put the `pulsar-io-amqp1_0-{version}.nar` in the pulsar connectors directory.

    ```
    cp pulsar-io-amqp1_0-{version}.nar $PULSAR_HOME/connectors/pulsar-io-amqp1_0-{version}.nar
    ```

3. Start Pulsar in standalone mode.

    ```
    $PULSAR_HOME/bin/pulsar standalone
    ```
   
    found logs like this
    ```
    Searching for connectors in /Volumes/other/apache-pulsar-2.8.0-SNAPSHOT/./connectors
    Found connector ConnectorDefinition(name=amqp1_0, description=AMQP1_0 source and sink connector, sourceClass=org.apache.pulsar.ecosystem.io.amqp.QpidJmsSource, sinkClass=org.apache.pulsar.ecosystem.io.amqp.QpidJmsSink, sourceConfigClass=null, sinkConfigClass=null) from /Volumes/other/apache-pulsar-2.8.0-SNAPSHOT/./connectors/pulsar-io-amqp1_0.nar
    ```

4. Create the AMQP1_0 sink.

    ```
    $PULSAR_HOME/bin/pulsar-admin sources create --source-config-file qpid-source-config.yaml
    ```
   
    found logs like this
    ```
    "Created successfully"
    ```
   
    get sinks list
    ```
    $PULSAR_HOME/bin/pulsar-admin sinks list
    ```
   
    found logs like this
    ```
    [
    "amqp1_0-sink"
    ]
    ```
   
    check sink status
    ```
    $PULSAR_HOME/bin/pulsar-admin sinks status --name amqp1_0-sink
    ```
   
    found logs like this
    ```
      "numInstances" : 1,
      "numRunning" : 1,
      "instances" : [ {
        "instanceId" : 0,
        "status" : {
          "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReadFromPulsar" : 0,
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "numSinkExceptions" : 0,
          "latestSinkExceptions" : [ ],
          "numWrittenToSink" : 0,
          "lastReceivedTime" : 0,
          "workerId" : "c-standalone-fw-localhost-8080"
        }
      } ]
    }
    ```

5. Send Pulsar messages.

    ```
    $PULSAR_HOME/bin/pulsar-client produce public/default/user-op-queue-topic --messages hello -n 10
    ```

6. Consume AMQP messages.

    Use this test method `receiveMessages` to consume AMQP messages.

    ```
    @Test
    public void receiveMessages() throws Exception {
        ConnectionFactory connectionFactory = new JmsConnectionFactory("guest", "guest", "amqp://localhost:5672");
        @Cleanup
        Connection connection = connectionFactory.createConnection();
        connection.start();
        @Cleanup
        Session session = connection.createSession();
        @Cleanup
        MessageConsumer consumer = session.createConsumer(new JmsQueue("user-op-queue-pulsar"));
        for (int i = 0; i < 10; i++) {
            JmsTextMessage textMessage = (JmsTextMessage) consumer.receive();
            System.out.println("receive msg content: " + textMessage.getText());
            textMessage.acknowledge();
        }
    }
    ```

7. Check the sink status.
   
    ```
    $PULSAR_HOME/bin/pulsar-admin sinks status --name amqp1_0-sink
    ```
    
    found logs like this
    ```
    {
      "numInstances" : 1,
      "numRunning" : 1,
      "instances" : [ {
        "instanceId" : 0,
        "status" : {
          "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReadFromPulsar" : 10,
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "numSinkExceptions" : 0,
          "latestSinkExceptions" : [ ],
          "numWrittenToSink" : 10,
          "lastReceivedTime" : 1615192471713,
          "workerId" : "c-standalone-fw-localhost-8080"
        }
      } ]
    }
    ```
