package main.java.pw.oliver.jmkb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class SchemaRegistryConnector {

	private final String schemaRegistryURI;
	
	public SchemaRegistryConnector(String schemaRegistryURI) {
		this.schemaRegistryURI = schemaRegistryURI;
		try {
			System.out.println(schemaRegistryURI + "/subjects");
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
			reader.lines().forEachOrdered(line->sb.append(line));
		
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
	
	public String getSchemaById(int id) {
		try {
			URL url = new URL(schemaRegistryURI + "/schemas/ids/" + id);
			return getSchema(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String getSchemaBySubject(String subject) {
		try {
			URL url = new URL(schemaRegistryURI + "/subjects/" + subject + "/versions/latest");
			return getSchema(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
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
