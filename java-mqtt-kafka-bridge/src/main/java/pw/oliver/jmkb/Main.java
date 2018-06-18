package main.java.pw.oliver.jmkb;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class is a bridge between a FROST-Server and Apache Kafka.
 * It serves as a MQTT consumer and an Apache Kafka Producer.
 * 
 * @author Oliver
 * 
 * @version 1.0
 */

public class Main {
	
	public static void main(String[] args) {
		
		String frostServerURI = "";
		String kafkaServerURI = "";
		String schemaRegistryURI = "";
		
		if (args.length == 0) {
			System.out.println("No arguments specified, using default ports 1883, 9092, 8081 on localhost");
			frostServerURI = "tcp://127.0.0.1:1883";
			kafkaServerURI = "127.0.0.1:9092";
			schemaRegistryURI = "http://127.0.0.1:8081";
		} else if (args.length == 3) {
			frostServerURI = args[0];
			kafkaServerURI = args[1];
			schemaRegistryURI = args[2];
			
			// prepend tcp:// to frostServerURI if no protocol is defined (required for MQTT)
			if (!frostServerURI.contains("://")) {
				frostServerURI = "tcp://" + frostServerURI;
			}
			
			// prepend http:// to schemaRegistryURI if no protocol is defined
			if (!schemaRegistryURI.contains("://")) {
				schemaRegistryURI = "http://" + schemaRegistryURI;
			}
			
			// check validity of URIs
			try {
				URI uriFrost  = new URI(frostServerURI);
				URI uriKafka  = new URI(kafkaServerURI);
				URI uriSchema = new URI(schemaRegistryURI);
				
				// check if port for FROST was specified
				if (uriFrost.getPort() == -1) {
					System.err.println("Bad URI format: No port defined for FROST-Server. Defaulting to port 1883");
					uriFrost = new URI(uriFrost.toString() + ":1883");
				}
				
				// check if port for Kafka was specified
				if (uriKafka.getPort() == -1) {
					System.err.println("Bad URI format: No port defined for Kafka Broker. Defaulting to port 9092");
					uriKafka = new URI(uriKafka.toString() + ":9092");
				}
				
				// check if port for Schema Registry was specified
				if (uriSchema.getPort() == -1) {
					System.err.println("Bad URI format: No port defined for the Schema Registry. Defaulting to port 8081");
					uriSchema = new URI(uriSchema.toString() + ":8081");
				}
				
				frostServerURI = uriFrost.toString();
				kafkaServerURI = uriKafka.toString();
				schemaRegistryURI = uriSchema.toString();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		} else {
			System.err.println("Invalid number of arguments: " + args.length);
			System.err.println("Correct usage:\n\tjava -jar jmkb.jar <frostServerURI> "
					+ "<kafkaBrokerURI> <schemaRegistryURI>");
			System.err.println("URI format is <address>:<port>");
			System.exit(-1);
		}
		
		System.out.println("FROST: " + frostServerURI
				+ "\nKafka: " + kafkaServerURI
				+ "\nSchema: " + schemaRegistryURI);
		
		MqttConsumer mqtt = new MqttConsumer(frostServerURI, "mqttconsumer1", kafkaServerURI, schemaRegistryURI);
		for (int i = 0; i <= 10; i++) {
			mqtt.testPublish("v1.0/HistoricalLocations", java.security.SecureRandom.getSeed(2048).toString());
		}
		mqtt.disconnect();
	}
	
}
