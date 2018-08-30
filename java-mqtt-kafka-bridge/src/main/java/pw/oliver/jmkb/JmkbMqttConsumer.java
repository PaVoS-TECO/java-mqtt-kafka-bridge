package pw.oliver.jmkb;

import org.apache.avro.specific.SpecificRecordBase;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the MQTT message consumer for the bridge.
 * It contains functionality to receive and process MQTT messages from a server defined in the properties file.
 * @author Oliver
 *
 */
public class JmkbMqttConsumer implements MqttCallback {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private String clientId;
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
		
		this.clientId = clientId;
		this.producer = producer;
		this.converter = new MqttMessageConverter();
		connect();
	}
	
	private void connect() {
		try {
			String frostServerURI = PropertiesFileReader.getProperty("frostServerURI");
			
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			this.client = new MqttClient(frostServerURI, clientId);
			client.setCallback(this);
			client.connect(options);
			logger.info(clientId + " successfully connected to MQTT");
			client.subscribe("v1.0/Things");
			client.subscribe("v1.0/Datastreams");
			client.subscribe("v1.0/Locations");
			client.subscribe("v1.0/HistoricalLocations");
			client.subscribe("v1.0/Sensors");
			client.subscribe("v1.0/ObservedProperties");
			client.subscribe("v1.0/FeaturesOfInterest");
			client.subscribe("v1.0/Observations");
			logger.info(clientId + " successfully subscribed to topics");
		} catch (MqttException e) {
			logger.warn("Could not initialize MQTT client " + clientId, e);
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.warn(clientId + "lost connection to MQTT");
		try {
			// prevent spamming connect() on connection loss
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		connect();
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		String messageTopic = topic;
		// remove "v1.0/" from topic
		if (messageTopic.contains("/")) {
			messageTopic = messageTopic.split("/")[1];
		} else {
			logger.warn("Received message topic " + messageTopic + " does not contain a slash! Message: " + message.toString());
			return;
		}
		// convert message and get key
		SpecificRecordBase avroMessage = converter.mqttMessageToAvro(messageTopic, message);
		if (avroMessage == null) {
			return;
		} else {
			String key = String.valueOf(avroMessage.get("iotId"));
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
			logger.info("MQTT consumer " + clientId + " has been closed");
		} catch (MqttException e) {
			logger.warn("Failed to close MQTT consumer " + clientId, e);
		}
	}
}
