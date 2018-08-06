package main.java.pw.oliver.jmkb;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.specific.SpecificRecordBase;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import main.java.pw.oliver.jmkb.avroclasses.Datastream;
import main.java.pw.oliver.jmkb.avroclasses.FeatureOfInterest;
import main.java.pw.oliver.jmkb.avroclasses.Location;
import main.java.pw.oliver.jmkb.avroclasses.LocationType;
import main.java.pw.oliver.jmkb.avroclasses.Observation;
import main.java.pw.oliver.jmkb.avroclasses.ObservedProperty;
import main.java.pw.oliver.jmkb.avroclasses.Sensor;
import main.java.pw.oliver.jmkb.avroclasses.Thing;
import main.java.pw.oliver.jmkb.avroclasses.UnitOfMeasurement;

public class MqttMessageConverter {

	private static final Logger logger = Logger.getLogger(MqttMessageConverter.class.getName());
	
	private FrostIotIdConverter conv;
	
	public MqttMessageConverter() {
		conv = new FrostIotIdConverter();
	}
	
	public SpecificRecordBase mqttMessageToAvro(String topic, MqttMessage message) {
		// This repo might contain a better implementation of this method:
		// https://github.com/allegro/json-avro-converter
		JSONObject m;
		try {
			m = (JSONObject) new JSONParser().parse(new String(message.getPayload()));
		} catch (ParseException e) {
			logger.log(Level.WARNING, e.toString(), e);
			return null;
		}
		SpecificRecordBase sr = null;
		switch (topic) {
			case "Datastreams":
				JSONObject uom = (JSONObject) m.get("unitOfMeasurement");
				sr = Datastream.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setName((String) m.get("name"))
				.setDescription((String) m.get("description"))
				.setUnitOfMeasurement(UnitOfMeasurement.newBuilder()
						.setName((String) uom.get("name"))
						.setDefinition((String) uom.get("definition"))
						.setSymbol((String) uom.get("symbol"))
						.build())
				.setObservationType((String) m.get("observationType"))
				.setObservedArea((String) m.get("observedArea"))
				.setPhenomenonTime((String) m.get("phenomenonTime"))
				.setResultTime((String) m.get("resultTime"))
				.setThing(conv.getSingleIotId((String) m.get("Thing@iot.navigationLink")))
				.setObservedProperty(conv.getSingleIotId((String) m.get("ObservedProperty@iot.navigationLink")))
				.setSensor(conv.getSingleIotId((String) m.get("Sensor@iot.navigationLink")))
				.setObservations(conv.getMultipleIotIds((String) m.get("Observations@iot.navigationLink")))
				.build();
				break;
			case "Sensors":
				sr = Sensor.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setName((String) m.get("name"))
				.setDescription((String) m.get("description"))
				.setEncodingType((String) m.get("encodingType"))
				.setMetadata((String) m.get("metadata"))
				.setDatastreams(conv.getMultipleIotIds((String) m.get("Datastreams@iot.navigationLink")))
				.build();
				break;
			case "ObservedProperties":
				sr = ObservedProperty.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setName((String) m.get("name"))
				.setDescription((String) m.get("description"))
				.setDefinition((String) m.get("definition"))
				.setDatastreams(conv.getMultipleIotIds((String) m.get("Datastreams@iot.navigationLink")))
				.build();
				break;
			case "Observations":
				sr = Observation.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setPhenomenonTime((String) m.get("phenomenonTime"))
				.setResult((String) m.get("result"))
				.setResultTime((String) m.get("resultTime"))
				.setResultQuality((String) m.get("resultQuality"))
				.setValidTime((String) m.get("validTime"))
				.setParameters((String) m.get("parameters"))
				.setDatastream(conv.getSingleIotId((String) m.get("Datastream@iot.navigationLink")))
				.setFeatureOfInterest(conv.getSingleIotId((String) m.get("FeatureOfInterest@iot.navigationLink")))
				.build();
				break;
			case "FeaturesOfInterest":
				JSONObject feat = (JSONObject) m.get("feature");
				String featCoordinates = ((JSONArray) feat.get("coordinates")).toString();
				// format the same way as iot.id lists
				if (featCoordinates != null && featCoordinates.length() >= 2) {
					featCoordinates = featCoordinates.substring(1, featCoordinates.length() - 1);
				} else {
					featCoordinates = "";
				}
				
				sr = FeatureOfInterest.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setName((String) m.get("name"))
				.setDescription((String) m.get("description"))
				.setEncodingType((String) m.get("encodingType"))
				.setFeature(LocationType.newBuilder()
						.setType((String) feat.get("type"))
						.setCoordinates(featCoordinates)
						.build())
				.setObservations(conv.getMultipleIotIds((String) m.get("Observations@iot.navigationLink")))
				.build();
				break;
			case "Things":
				sr = Thing.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setName((String) m.get("name"))
				.setDescription((String) m.get("description"))
				.setLocations(conv.getMultipleIotIds((String) m.get("Locations@iot.navigationLink")))
				//.setHistoricalLocations(conv.getMultipleIotIds((String) m.get("HistoricalLocations@iot.navigationLink")))
				.setDatastreams(conv.getMultipleIotIds((String) m.get("Datastreams@iot.navigationLink")))
				.build();
				break;
			case "HistoricalLocations":
				sr = null;
				break;
			case "Locations":
				JSONObject loc = (JSONObject) m.get("feature");
				String locCoordinates = ((JSONArray) loc.get("coordinates")).toString();
				// format the same way as iot.id lists
				if (locCoordinates != null && locCoordinates.length() >= 2) {
					locCoordinates = locCoordinates.substring(1, locCoordinates.length() - 1);
				} else {
					locCoordinates = "";
				}
				
				sr = Location.newBuilder()
				.setIotId(m.get("@iot.id").toString())
				.setName((String) m.get("name"))
				.setDescription((String) m.get("description"))
				.setEncodingType((String) m.get("encodingType"))
				.setLocation(LocationType.newBuilder()
						.setType((String) loc.get("type"))
						.setCoordinates(locCoordinates)
						.build())
				.setThings(conv.getMultipleIotIds((String) m.get("Things@iot.navigationLink")))
				.build();
				break;
			default:
				sr = null;
				break;
		}
		return sr;
	}
}
