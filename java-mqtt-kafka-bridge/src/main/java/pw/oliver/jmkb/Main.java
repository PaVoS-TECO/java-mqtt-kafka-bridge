package pw.oliver.jmkb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	// prevent unwanted instantiation of utility class
	private Main() {
		throw new AssertionError("Instantiating utility class!");
	}
	
	/**
	 * The main class. Initializes required classes and then enters an infinite loop waiting for new MQTT messages.
	 * @param args Parameters given to the main class. Set args[0] to "test" for testing (no while loop)
	 */
	public static void main(String[] args) {
		
		boolean initStatus = PropertiesFileReader.init();
		if (!initStatus) {
			logger.warn("There was an error reading the properties file. The bridge was not started.");
			System.exit(-1);
		}
		JmkbKafkaProducer producer = new JmkbKafkaProducer();
		JmkbMqttConsumer consumer = new JmkbMqttConsumer("mqttconsumer1", producer);
		
		// set shutdown hook so that program can terminate gracefully when user presses Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				Logger shlogger = LoggerFactory.getLogger(this.getClass());
				shlogger.info("Performing shutdown.");
				consumer.disconnect();
				producer.disconnect();
			}
		});
		
		logger.info("The bridge is now running, terminate with Ctrl+C.");
		
		if ((args.length > 0) && (args[0].equals("test"))) {
			return;
		}
		
		// infinite loop to keep bridge running, can be interrupted with Ctrl+C.
		while (initStatus) {
			// wait for messages to process
		}
	}
	
}
