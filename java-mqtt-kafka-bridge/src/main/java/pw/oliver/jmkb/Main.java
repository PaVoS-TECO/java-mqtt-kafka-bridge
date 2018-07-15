package main.java.pw.oliver.jmkb;

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
		
		// for logging
		
		// initialize PropertiesFileReader
		PropertiesFileReader.init();
		
		JmkbKafkaProducer producer = new JmkbKafkaProducer();
		JmkbMqttConsumer consumer = new JmkbMqttConsumer("mqttconsumer1", producer);
		
		// set shutdown hook so that program can terminate gracefully when user presses Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Performing shutdown.");
				consumer.disconnect();
				System.out.println("MQTT consumer shutdown.");
				producer.disconnect();
				System.out.println("Kafka producer shutdown.");
				System.out.println("Shutdown complete.");
			}
		});
		
		System.out.println("The bridge is now running, terminate with Ctrl+C.");
		
		/*
		 * Uncomment for testing
		 * 
		for (int i = 0; i <= 10; i++) {
			consumer.testPublish("v1.0/HistoricalLocations", "{\r\n" + 
					"    \"glossary\": {\r\n" + 
					"        \"title\": \"example glossary\",\r\n" + 
					"		\"GlossDiv\": {\r\n" + 
					"            \"title\": \"S\",\r\n" + 
					"			\"GlossList\": {\r\n" + 
					"                \"GlossEntry\": {\r\n" + 
					"                    \"ID\": \"SGML\",\r\n" + 
					"					\"SortAs\": \"SGML\",\r\n" + 
					"					\"GlossTerm\": \"Standard Generalized Markup Language\",\r\n" + 
					"					\"Acronym\": \"SGML\",\r\n" + 
					"					\"@iot.id\": \"d463saf8a43asd41f8\",\r\n" + 
					"					\"Abbrev\": \"ISO 8879:1986\",\r\n" + 
					"					\"GlossDef\": {\r\n" + 
					"                        \"para\": \"A meta-markup language, used to create markup languages such as DocBook.\",\r\n" + 
					"						\"GlossSeeAlso\": [\"GML\", \"XML\"]\r\n" + 
					"                    },\r\n" + 
					"					\"GlossSee\": \"markup\"\r\n" + 
					"                }\r\n" + 
					"            }\r\n" + 
					"        }\r\n" + 
					"    }\r\n" + 
					"}");
		}*/
		
		while(true);
	}
	
}
