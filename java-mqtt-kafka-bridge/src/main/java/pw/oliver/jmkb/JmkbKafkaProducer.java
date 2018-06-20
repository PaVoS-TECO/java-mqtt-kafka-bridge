package main.java.pw.oliver.jmkb;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class JmkbKafkaProducer {
	
	private KafkaProducer<String, byte[]> producer;
	
	public JmkbKafkaProducer(String kafkaServerURI, String schemaRegistryURI) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", kafkaServerURI);
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		properties.put("schema.registry.url", schemaRegistryURI);
		producer = new KafkaProducer<>(properties);
		//producer.send(new ProducerRecord<String, byte[]>("v1.0/Locations", "0".getBytes()));
		System.out.println(producer.toString());
	}
	
	public void send(String topic, byte[] avroMessage) {
		producer.send(new ProducerRecord<String, byte[]>(topic, avroMessage));
	}
}
