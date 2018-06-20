package main.java.pw.oliver.jmkb;

import org.apache.avro.Schema;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttMessageConverter {

	private Schema.Parser parser;
	
	public MqttMessageConverter(String schemaRegistryURI) {
		parser = new Schema.Parser();
		SchemaRegistryConnector connector = new SchemaRegistryConnector(schemaRegistryURI);
		parser.parse(connector.getSchemaById(9));
	}
	
	public byte[] mqttMessageToAvro(MqttMessage message) {
		return message.getPayload();
	}
}
