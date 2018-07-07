# java-mqtt-kafka-bridge
A bridge between a FROST Server and Apache Kafka, written in Java

## Build from source
Requirements:
- A working JDK. This program is not guaranteed to work properly with JDK versions below 8u171.
- Maven
- Git

On Debian/Ubuntu systems, just run `sudo apt install default-jdk maven git`.
1. Clone this repository via `git clone https://github.com/olivermliu/java-mqtt-kafka-bridge.git`.
2. `cd java-mqtt-kafka-bridge/java-mqtt-kafka-bridge`
3. `mvn clean install`
4. Currently, the generated .jar has a bug. To run the program, please use `mvn exec:java -D exec.mainClass=main.java.pw.oliver.jmkb.main`.

## Configuration
Set configurables in the jmkb.properties file. Please note: URIs require a `<protocol>://<address>:<port>` format.

Currently, configurables are:
- `frostServerURI`: the URI from which to get the MQTT messages of the FROST-Server. Requires `tcp://` as protocol. Usually port 1883.
- `kafkaBrokerURI`: the URI to which to send Kafka records. With Kafka Landoop, use the port defined under "Kafka Broker". Usually port 9092 and `http://` as protocol.
- `schemaRegistryURI`: the URI from which to get Avro schemas. With Kafka Landoop, use the port defined under "Schema Registry". Usually port 8081 and `http://` as protocol.
- `schemaId`: the ID of the schema to use.

## Further information
- Use `Ctrl+C` to terminate the program. This ensures that the MQTT Client and Kafka Producer disconnect properly.
