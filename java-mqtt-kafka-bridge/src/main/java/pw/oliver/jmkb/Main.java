package pw.oliver.jmkb;

/**
 * This is the main class for the bridge between a FROST-Server and Apache Kafka.
 * It initializes a Kafka producer and a MQTT consumer.
 * The bridge can be gracefully stopped via the Ctrl+C combination.
 * This calls {@link JmkbKafkaProducer#disconnect()} and {@link JmkbMqttConsumer#disconnect()}
 * and subsequently terminates the program.
 * 
 * @author Oliver
 * 
 * @version 1.0
 */

public final class Main {
	
	// prevent unwanted instantiation of utility class
	private Main() {
		throw new AssertionError("Instantiating utility class!");
	}
	
	/**
	 * The main class. Initializes required classes and then enters an infinite loop waiting for new MQTT messages.
	 * @param args Parameters given to the main class (unused)
	 */
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
		while (true) {
			// wait for messages to process
		}
	}
	
}
