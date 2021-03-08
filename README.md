### AMQP1_0 Source Doc

see the docs/io-amqp1_0-source.md

### AMQP1_0 Source Doc

see the docs/io-amqp1_0-sink.md

### Project Layout

Before starting developing your own connector, please take a look at
how this template project organize the files for a connector.

```bash

├── conf
├── docs
├── src
│   ├── checkstyle
│   ├── license
│   │   └── ALv2
│   ├── main
│   │   └── java
│   ├── spotbugs
│   └── test
│       └── java

```

- `conf` directory is used for storing examples of config files of this connector.
- `docs` directory is used for keeping the documentation of this connector.
- `src` directory is used for storing the source code of this connector.
  - `src/checkstyle`: store the checkstyle configuration files
  - `src/license`: store the license header for this project. `mvn license:format` can
    be used for formatting the project with the stored license header in this directory.
  - `src/spotbugs`: store the spotbugs configuration files
  - `src/main`: for keeping all the main source files
  - `src/test`: for keeping all the related tests
