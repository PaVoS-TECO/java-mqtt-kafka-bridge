package pavos.jmkb;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class is a bridge between a FROST-Server and Apache Kafka.
 * It serves as a MQTT consumer and an Apache Kafka Producer.<br><br>
 * 
 * Run this program with arguments: <code>Bridge &lt;frostServerURI&gt; &lt;kafkaServerURI&gt;</code><br>
 * If this program is run without arguments, it will default to: <code>Bridge localhost:1883 localhost:9092</code><br>
 * A valid URI should have the format <code>&lt;address&gt;:&lt;port&gt;</code>.
 * 
 * @author Oliver
 * 
 * @version 1.0
 */

public class Main {
	
	public static void main(String[] args) {
		
		String frostServerURI = "";
		String kafkaServerURI = "";
		
		if (args.length == 0) {
			frostServerURI = "tcp://127.0.0.1:1883";
			kafkaServerURI = "127.0.0.1:9092";
		} else if (args.length == 2) {
			frostServerURI = args[0];
			kafkaServerURI = args[1];
			
			// check validity of frostServerURI, prepend tcp:// if no protocol is defined (required for MQTT)
			if (!frostServerURI.contains("://")) {
				frostServerURI = "tcp://" + frostServerURI;
			}
			
			try {
				URI uriFrost = new URI(frostServerURI);
				URI uriKafka = new URI(kafkaServerURI);
				
				// check if port for FROST was specified
				if (uriFrost.getPort() == -1) {
					System.err.println("Bad URI format: Please specify the port to be used for FROST. Defaulting to port 1883");
					uriFrost = new URI(uriFrost.toString() + ":1883");
				}
				
				// check if port for Kafka was specified
				if (uriKafka.getPort() == -1) {
					System.err.println("Bad URI format: Please specify the port to be used for Kafka. Defaulting to port 9092");
					uriKafka = new URI(uriKafka.toString() + ":9092");
				}
				
				frostServerURI = uriFrost.toString();
				kafkaServerURI = uriKafka.toString();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		} else {
			throw new IllegalArgumentException("Invalid number of arguments: " + args.length);
		}
		
		System.out.println(frostServerURI + "\n" + kafkaServerURI);
		
		MqttConsumer mqtt = new MqttConsumer(frostServerURI, "mqttconsumer1");
		while (true) {
			mqtt.testPublish("v1.0/Things", "TESTING");
			System.out.println("A");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
