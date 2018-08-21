package main.java.pw.oliver.jmkb;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.specific.SpecificRecordBase;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * This class is the MQTT message consumer for the bridge.
 * It contains functionality to receive and process MQTT messages from a server defined in the properties file.
 * @author Oliver
 *
 */
public class JmkbMqttConsumer implements MqttCallback {

	private static final Logger LOGGER = Logger.getLogger(JmkbMqttConsumer.class.getName());
	
	private MqttClient client;
	private JmkbKafkaProducer producer;
	private MqttMessageConverter converter;
	
	/**
	 * This class is the MQTT consumer for the bridge.
	 * It contains functionality to receive MQTT messages from the FROST server defined
	 * in the properties file.<br>
	 * Note that multiple instances of this class with the same clientId will behave the
	 * same, possibly leading to duplicated Kafka records being sent.
	 * @param clientId The unique String identifier of this clientId.
	 * @param producer The JmkbKafkaProducer at which the received message should be sent
	 */
	public JmkbMqttConsumer(String clientId, JmkbKafkaProducer producer) {
		
		// Initialize new message converter
		converter = new MqttMessageConverter();
		
		// Initialize new MQTT Client
		try {
			this.producer = producer;
			String frostServerURI = PropertiesFileReader.getProperty("frostServerURI");
			
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			this.client = new MqttClient(frostServerURI, clientId);
			client.setCallback(this);
			client.connect(options);
			System.out.println("Successfully connected to MQTT");
			client.subscribe("v1.0/Things");
			client.subscribe("v1.0/Datastreams");
			client.subscribe("v1.0/Locations");
			client.subscribe("v1.0/HistoricalLocations");
			client.subscribe("v1.0/Sensors");
			client.subscribe("v1.0/ObservedProperties");
			client.subscribe("v1.0/FeaturesOfInterest");
			client.subscribe("v1.0/Observations");
			System.out.println("Successfully subscribed to topics");
		} catch (MqttException e) {
			LOGGER.log(Level.SEVERE, e.toString(), e);
		}
	}
	
	/* Uncomment this method for testing
	public void testPublish(String topic, String message) {
		try {
			MqttTopic mqttTopic = client.getTopic(topic);
			MqttDeliveryToken token = mqttTopic.publish(new MqttMessage(message.getBytes()));
			token.waitForCompletion(1000);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}*/

	@Override
	public void connectionLost(Throwable cause) {
		LOGGER.log(Level.WARNING, "Connection to MQTT lost!");
		try {
			// close client and quit bridge on connection loss
			client.close();
		} catch (MqttException e) {
			LOGGER.log(Level.SEVERE, e.toString(), e);
		}
		System.exit(-1);
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		String messageTopic = topic;
		// remove "v1.0/" from topic
		if (messageTopic.contains("/")) {
			messageTopic = messageTopic.split("/")[1];
		} else {
			LOGGER.log(Level.WARNING, "JmkbMqttConsumer: Received message topic does not contain a slash!");
			LOGGER.log(Level.INFO, "JmkbMqttConsumer: " + messageTopic + " - " + message.toString());
			messageTopic = "ErrorTopicNoSlashFound";
		}
		// convert message and get key
		SpecificRecordBase avroMessage = converter.mqttMessageToAvro(messageTopic, message);
		if (avroMessage == null) {
			return;
		} else {
			String key = (String) avroMessage.get("iotId");
			producer.send(messageTopic, key, avroMessage);
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// will not use, since we only consume
	}
	
	/**
	 * Disconnects the MQTT consumer from the server. This effectively destroys the consumer.
	 * New MQTT messages arriving after this method is called will not be processed.
	 */
	public void disconnect() {
		try {
			client.disconnect();
			client.close();
		} catch (MqttException e) {
			LOGGER.log(Level.SEVERE, e.toString(), e);
		}
	}
}
