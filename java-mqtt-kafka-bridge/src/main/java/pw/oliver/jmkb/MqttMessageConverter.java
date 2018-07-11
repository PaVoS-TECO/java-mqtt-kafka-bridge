package main.java.pw.oliver.jmkb;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MqttMessageConverter {
	
	public static byte[] mqttMessageToAvro(MqttMessage message, String schema) {
		return message.getPayload();
	}
	
	public static String getSensorIdFromMessage(MqttMessage message) {
		try {
			JSONObject jo = (JSONObject) new JSONParser().parse(message.toString());
			if (jo.containsKey("@iot.id")) {
				return jo.get("@iot.id").toString();
			} else {
				return null;
			}
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
}
