package pw.oliver.jmkb;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * This class is the Kafka record producer for the bridge.
 * It contains functionality to send Kafka records to the Kafka broker defined in the properties file.
 * @author Oliver
 *
 */
public class JmkbKafkaProducer {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private KafkaProducer<String, SpecificRecordBase> producer;
	
	/**
	 * Constructor. Initializes the Kafka producer.
	 */
	public JmkbKafkaProducer() {
		String kafkaBrokerURI = PropertiesFileReader.getProperty("kafkaBrokerURI");
		String schemaRegistryURI = PropertiesFileReader.getProperty("schemaRegistryURI");
		Properties properties = new Properties();
		properties.put("bootstrap.servers", kafkaBrokerURI);
		properties.put("acks", "all");
		properties.put("linger.ms", 10);
		properties.put("retries", 0);
		properties.put("key.serializer", StringSerializer.class.getName());
		properties.put("value.serializer", KafkaAvroSerializer.class.getName());
		properties.put("schema.registry.url", schemaRegistryURI);
		properties.put("max.block.ms", 10000);
		producer = new KafkaProducer<>(properties);
		logger.debug("Finished initializing Kafka producer");
	}
	
	/**
	 * Creates a Kafka record based on the given topic, key and SpecificRecordBase object.
	 * @param topic The topic of the record
	 * @param key The key of the record
	 * @param avroMessage The SpecificRecordBase to send
	 */
	public void send(String topic, String key, SpecificRecordBase avroMessage) {
		producer.send(new ProducerRecord<String, SpecificRecordBase>(topic, key, avroMessage));
		producer.flush();
	}
	
	/**
	 * Disconnects the Kafka producer from the Kafka broker. This effectively destroys the producer.
	 * Subsequent calls to {@link #send(String, String, SpecificRecordBase)} will not work.
	 */
	public void disconnect() {
		producer.close(2, TimeUnit.SECONDS);
		logger.info("Kafka producer has been closed");
	}
}
