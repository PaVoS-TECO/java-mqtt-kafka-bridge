package main.java.pw.oliver.jmkb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Helper class to connect and interact with the Avro schema registry.
 * @author Oliver
 *
 */
public class SchemaRegistryConnector {
	
	private final String schemaRegistryURI;
	
	/**
	 * Constructor. Tests if a connection to the schema registry can be established.
	 */
	public SchemaRegistryConnector() {
		schemaRegistryURI = PropertiesFileReader.getProperty("schemaRegistryURI");
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(schemaRegistryURI + "/subjects").openConnection();
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Could not establish connection to schema registry! "
						+ "HTTP response code: " + conn.getResponseCode());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private String getSchema(URL url) {
		try {
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200) {
				return null;
			}
		
			BufferedReader reader = new BufferedReader((new InputStreamReader(conn.getInputStream())));
			StringBuilder sb = new StringBuilder();
			reader.lines().forEachOrdered(sb::append);
		
			JSONObject jo = (JSONObject) new JSONParser().parse(sb.toString());
			return jo.get("schema").toString();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Get the schema with the specified id.
	 * @param id The id of the schema
	 * @return The schema
	 */
	public String getSchemaById(int id) {
		try {
			URL url = new URL(schemaRegistryURI + "/schemas/ids/" + id);
			return getSchema(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Get the latest version of the schema with the specified subject.
	 * @param subject The subject of the schema
	 * @return The schema
	 */
	public String getSchemaBySubject(String subject) {
		try {
			URL url = new URL(schemaRegistryURI + "/subjects/" + subject + "/versions/latest");
			return getSchema(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Get the specified version of the schema with the specified subject.
	 * @param subject The subject of the schema
	 * @param version The version of the schema
	 * @return The schema
	 */
	public String getSchemaBySubject(String subject, int version) {
		try {
			URL url = new URL(schemaRegistryURI + "/subjects/" + subject + "/versions/" + version);
			return getSchema(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
}
