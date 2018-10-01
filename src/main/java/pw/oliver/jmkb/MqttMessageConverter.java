package pw.oliver.jmkb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.specific.SpecificRecordBase;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import pw.oliver.jmkb.avroclasses.Datastream;
import pw.oliver.jmkb.avroclasses.FeatureOfInterest;
import pw.oliver.jmkb.avroclasses.Location;
import pw.oliver.jmkb.avroclasses.LocationType;
import pw.oliver.jmkb.avroclasses.Observation;
import pw.oliver.jmkb.avroclasses.ObservedProperty;
import pw.oliver.jmkb.avroclasses.Sensor;
import pw.oliver.jmkb.avroclasses.Thing;
import pw.oliver.jmkb.avroclasses.UnitOfMeasurement;

/**
 * This class handles the conversion of MqttMessages to a different format.
 * @author Oliver
 *
 */
public class MqttMessageConverter {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private FrostIotIdConverter conv;

	/**
	 * Default constructor.
	 */
	public MqttMessageConverter() {
		conv = new FrostIotIdConverter();
	}

	/**
	 * Converts an MqttMessage to a SpecificRecordBase object based on the type of FROST topic.
	 * The returned object will actually be an instance of one of the seven classes defined in the
	 * avroclasses subpackage.
	 * @param topic The topic of the MqttMessage
	 * @param message The MqttMessage
	 * @return a SpecificRecordBase object
	 */
	public SpecificRecordBase mqttMessageToAvro(String topic, MqttMessage message) {
		// This repo might contain a better implementation of this method:
		// https://github.com/allegro/json-avro-converter
		JsonObject m;
		try {
			m = (JsonObject) new JsonParser().parse(new String(message.getPayload()));
		} catch (JsonParseException e) {
			logger.warn("Error parsing MQTT message payload as JsonObject", e);
			return null;
		}
		SpecificRecordBase sr = null;
		try {
			switch (topic) {
			case "Datastreams":
				JsonObject uom = (JsonObject) m.get("unitOfMeasurement");
				sr = Datastream.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setName(m.get("name").toString())
						.setDescription(m.get("description").toString())
						.setUnitOfMeasurement(UnitOfMeasurement.newBuilder()
								.setName(uom.get("name").toString())
								.setDefinition(uom.get("definition").toString())
								.setSymbol(uom.get("symbol").toString())
								.build())
						.setObservationType(m.get("observationType").toString())
						.setObservedArea((m.get("observedArea") == null) ? null : m.get("observedArea").toString())
						.setPhenomenonTime((m.get("phenomenonTime") == null) ? null : m.get("phenomenonTime").toString())
						.setResultTime((m.get("resultTime") == null) ? null : m.get("resultTime").toString())
						.setThing(conv.getIotIds(m.get("Thing@iot.navigationLink").toString()))
						.setObservedProperty(conv.getIotIds(m.get("ObservedProperty@iot.navigationLink").toString()))
						.setSensor(conv.getIotIds(m.get("Sensor@iot.navigationLink").toString()))
						.setObservations((m.get("Observations@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Observations@iot.navigationLink").toString()))
						.build();
				break;
			case "Sensors":
				sr = Sensor.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setName(m.get("name").toString())
						.setDescription(m.get("description").toString())
						.setEncodingType(m.get("encodingType").toString())
						.setMetadata(m.get("metadata").toString())
						.setDatastreams((m.get("Datastreams@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Datastreams@iot.navigationLink").toString()))
						.build();
				break;
			case "ObservedProperties":
				sr = ObservedProperty.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setName(m.get("name").toString())
						.setDescription(m.get("description").toString())
						.setDefinition(m.get("definition").toString())
						.setDatastreams((m.get("Datastreams@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Datastreams@iot.navigationLink").toString()))
						.build();
				break;
			case "Observations":
				sr = Observation.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setPhenomenonTime(m.get("phenomenonTime").toString())
						.setResult(m.get("result").toString())
						.setResultTime(m.get("resultTime").toString())
						.setResultQuality((m.get("resultQuality") == null) ? null : m.get("resultQuality").toString())
						.setValidTime((m.get("validTime") == null) ? null : m.get("validTime").toString())
						.setDatastream(conv.getIotIds(m.get("Datastream@iot.navigationLink").toString()))
						.setFeatureOfInterest((m.get("FeatureOfInterest@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("FeatureOfInterest@iot.navigationLink").toString()))
						.build();
				break;
			case "FeaturesOfInterest":
				JsonObject feat = (JsonObject) m.get("feature");
				String featCoordinates = ((JsonArray) feat.get("coordinates")).toString();
				// format the same way as iot.id lists
				if (featCoordinates != null && featCoordinates.length() >= 2) {
					featCoordinates = featCoordinates.substring(1, featCoordinates.length() - 1);
				} else {
					featCoordinates = "";
				}

				sr = FeatureOfInterest.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setName(m.get("name").toString())
						.setDescription(m.get("description").toString())
						.setEncodingType(m.get("encodingType").toString())
						.setFeature(LocationType.newBuilder()
								.setType(feat.get("type").toString())
								.setCoordinates(featCoordinates)
								.build())
						.setObservations((m.get("Observations@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Observations@iot.navigationLink").toString()))
						.build();
				break;
			case "Things":
				sr = Thing.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setName(m.get("name").toString())
						.setDescription(m.get("description").toString())
						.setLocations((m.get("Locations@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Locations@iot.navigationLink").toString()))
						.setDatastreams((m.get("Datastreams@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Datastreams@iot.navigationLink").toString()))
						.build();
				break;
			case "HistoricalLocations":
				sr = null;
				break;
			case "Locations":
				JsonObject loc = (JsonObject) m.get("feature");
				String locCoordinates = ((JsonArray) loc.get("coordinates")).toString();
				// format the same way as iot.id lists
				if (locCoordinates != null && locCoordinates.length() >= 2) {
					locCoordinates = locCoordinates.substring(1, locCoordinates.length() - 1);
				} else {
					locCoordinates = "";
				}

				sr = Location.newBuilder()
						.setIotId(m.get("@iot.id").toString())
						.setName(m.get("name").toString())
						.setDescription(m.get("description").toString())
						.setEncodingType(m.get("encodingType").toString())
						.setLocation(LocationType.newBuilder()
								.setType(loc.get("type").toString())
								.setCoordinates(locCoordinates)
								.build())
						.setThings((m.get("Things@iot.navigationLink") == null) ? null : conv.getIotIds(m.get("Things@iot.navigationLink").toString()))
						.build();
				break;
			default:
				sr = null;
				break;
			}
		} catch (NullPointerException e) {
			logger.warn("NullPointerException while building SpecificRecordBase, likely due to missing required entry.", e);
			return null;
		}
		return sr;
	}

	public String mqttMessageToJson(String messageTopic, MqttMessage message) {
		try {
			JsonObject jo = new JsonParser().parse(new String(message.getPayload())).getAsJsonObject();
			Iterator<Entry<String, JsonElement>> it = jo.entrySet().iterator();
			Map<String, String> newMap = new HashMap<>();
			while(it.hasNext()) {
				Entry<String, JsonElement> entry = it.next();
				if (entry.getKey().endsWith("@iot.navigationLink")) {
					// remove @iot.navigationLink from key
					String newKey = entry.getKey().split("@")[0];
					String newValue = conv.getIotIds(entry.getValue().getAsString());
					newMap.put(newKey, newValue);
				} else {
					newMap.put(entry.getKey(), entry.getValue().getAsString());
				}
			}
			return new Gson().toJson(newMap);
		} catch (JsonParseException | IllegalStateException e) {
			logger.warn("Error parsing given message", e);
		}
		return null;
	}
	
	public String getKeyFromMessage(MqttMessage message) {
		try {
			return String.valueOf(((JsonObject) new JsonParser().parse(new String(message.getPayload()))).get("@iot.id"));
		} catch (JsonParseException e) {
			logger.warn("Error parsing given message", e);
		}
		return null;
	}
}
