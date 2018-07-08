package main.java.pw.oliver.jmkb;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class JmkbKafkaProducer {
	
	private KafkaProducer<String, byte[]> producer;
	
	public JmkbKafkaProducer() {
		String kafkaBrokerURI = PropertiesFileReader.getProperty("kafkaBrokerURI");
		String schemaRegistryURI = PropertiesFileReader.getProperty("schemaRegistryURI");
		Properties properties = new Properties();
		properties.put("bootstrap.servers", kafkaBrokerURI);
		properties.put("acks", "all");
		properties.put("linger.ms", 10);
		properties.put("retries", 0);
		properties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		properties.put("schema.registry.url", schemaRegistryURI);
		properties.put("max.block.ms", 10000);
		producer = new KafkaProducer<>(properties);
	}
	
	public void send(String topic, String key, byte[] avroMessage) {
		Future<RecordMetadata> status = producer.send(new ProducerRecord<String, byte[]>(topic, key, avroMessage));
		try {
			RecordMetadata statusMetadata = status.get();
			System.out.println("----------------------------------");
			System.out.println("[SEND]\tKey: " + key);
			System.out.println("[SEND]\tTopic: " + statusMetadata.topic());
			System.out.println("[SEND]\tPartition: " + statusMetadata.partition());
			System.out.println("[SEND]\tTimestamp: " + statusMetadata.timestamp());
			System.out.println("[SEND]\tSerializedKeySize: " + statusMetadata.serializedKeySize());
			System.out.println("[SEND]\tSerializedValueSize: " + statusMetadata.serializedValueSize());
			System.out.println("----------------------------------");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public void disconnect() {
		producer.close(2, TimeUnit.SECONDS);
	}
}
