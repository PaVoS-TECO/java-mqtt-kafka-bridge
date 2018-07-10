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
4. Currently, the generated .jar has a bug. To run the program, please use `mvn exec:java -D exec.mainClass=main.java.pw.oliver.jmkb.Main`.

## Configuration
Set configurables in the jmkb.properties file. Please note: URIs require a `<protocol>://<address>:<port>` format.

Currently, configurables are:
- `frostServerURI`: the URI from which to get the MQTT messages of the FROST-Server. Requires `tcp://` as protocol. Usually port 1883.
- `kafkaBrokerURI`: the URI to which to send Kafka records. With Kafka Landoop, use the port defined under "Kafka Broker". Usually port 9092 and `http://` as protocol.
- `schemaRegistryURI`: the URI from which to get Avro schemas. With Kafka Landoop, use the port defined under "Schema Registry". Usually port 8081 and `http://` as protocol.
- `schemaId`: the ID of the schema to use.

## Further information
- Use `Ctrl+C` to terminate the program. This ensures that the MQTT Client and Kafka Producer disconnect properly.

## Test data
Test data are provided in the password-protected archive TestData.7z. With this, the functionality of the program can be tested. Please make sure the bridge is running before you start step 4. You will need Python 3 to run the scripts and p7zip-full to extract the test data.

1. Run `sudo apt install p7zip-full python3 python3-pip` to install Python 3, pip and 7z.
2. Run `python3 -m pip install pandas requests` to install required modules.
3. Run `7z x TestData.7z` in the directory of `TestData.7z` to extract the scripts and test data. Enter the password when prompted.
4. Run `python3 CreateThing.py`, then `python3 CreateDatastream.py`, then `python3 AddObservationsToDatastream.py`.
5. Test data should have been successfully published to FROST.
