package main.java.pw.oliver.jmkb;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;

public class JmkbKafkaProducer {
	
	public JmkbKafkaProducer(String kafkaServerURI, String schemaRegistryURI) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", kafkaServerURI.toString());
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("schema.registry.url", schemaRegistryURI);
		
		Schema.Parser parser = new Schema.Parser();
		KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
		System.out.println(producer.toString());
		producer.close();
	}
	
}
