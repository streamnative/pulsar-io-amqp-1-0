# AMQP1_0 connector

The AMQP1_0 connector is a [Pulsar IO connector](http://pulsar.apache.org/docs/en/next/io-overview/) for exchanging data between [AMQP 1.0](https://www.amqp.org/) and [Pulsar](https://pulsar.apache.org/docs/en/next/standalone/). It consists of two types of connectors: 

- AMQP1_0 **source** connector 
  
  This connector feeds data from AMQP and writes data to Pulsar topics. 

  ![](docs/amqp-1-0-source.png)

- AMQP1_0 **sink** connector 
  
  This connector pulls data from Pulsar topics and persists data to AMQP.

  ![](docs/amqp-1-0-sink.png)
            
## Doc

| AMQP1_0 connector version |Apache Pulsar version|Doc |
| :---------- |  :------------- |:------------- |
2.7.1.1 <br><br> 2.7.1.2 <br><br> 2.7.1.3| 2.7.x |- [AMQP1_0 source connector](https://github.com/streamnative/pulsar-io-amqp-1-0/blob/branch-2.7.1/docs/amqp-1-0-source.md)<br><br>  - [AMQP1_0 sink connector](https://github.com/streamnative/pulsar-io-amqp-1-0/blob/branch-2.7.1/docs/amqp-1-0-sink.md)

## Project layout

Below are the sub folders and files of this project and their corresponding descriptions.

```bash
├── conf // examples of configuration files of this connector
├── docs // user guides of this connector
├── script // scripts of this connector
├── io-amqp1_0-impl
│   ├── src // source code of this connector
├── tests // integration test
├── src
│   ├── checkstyle // checkstyle configuration files of this connector
│   ├── license // license header for this project. `mvn license:format` can be used for formatting the project with the stored license header in this directory
│   │   └── ALv2
│   ├── spotbugs // spotbugs configuration files of this connector
```
