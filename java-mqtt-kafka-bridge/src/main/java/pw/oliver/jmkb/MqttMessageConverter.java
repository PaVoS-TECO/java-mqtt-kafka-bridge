package main.java.pw.oliver.jmkb;

import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import main.java.pw.oliver.jmkb.avroclasses.Datastream;

public class MqttMessageConverter {
	
	public static SpecificRecordBase mqttMessageToAvro(MqttMessage message, String schema) {
		SpecificRecordBase record = Datastream.newBuilder().setName("TestDatastream").build();
		AvroSerializer<SpecificRecordBase> as = new AvroSerializer<>(record.getSchema());
		System.out.println(record.getClass());
		return record;
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
