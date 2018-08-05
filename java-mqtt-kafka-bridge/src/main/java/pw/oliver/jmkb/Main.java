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
		
		// infinite loop to keep bridge running, can be interrupted with Ctrl+C.
		while(true);
	}
	
}
