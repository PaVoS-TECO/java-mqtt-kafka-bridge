package main.java.pw.oliver.jmkb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FrostIotIdConverter {

	private static final Logger logger = Logger.getLogger(FrostIotIdConverter.class.getName());
	
	public FrostIotIdConverter() {
		
	}
	
	public String getSingleIotId(String link) {
		if (link == null) {
			return null;
		}
		return getJSONObjectFromNavigationLink(link).get("@iot.id").toString();
	}

	public String getMultipleIotIds(String link) {
		if (link == null) {
			return null;
		}
		LinkedList<String> ll = new LinkedList<>();
		JSONObject jo = getJSONObjectFromNavigationLink(link);
		JSONArray ja = (JSONArray) jo.get("value");
		Iterator<?> it = ja.iterator();
		while (it.hasNext()) {
			ll.add(((JSONObject) it.next()).get("@iot.id").toString());
		}
		String ids = String.join(",", ll);
		return ids;
	}

	private JSONObject getJSONObjectFromNavigationLink(String link) {
		try {
			URL url = new URL(link);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200) {
				return null;
			} else {
				BufferedReader reader = new BufferedReader((new InputStreamReader(conn.getInputStream())));
				StringBuilder sb = new StringBuilder();
				reader.lines().forEachOrdered(sb::append);
			
				return (JSONObject) new JSONParser().parse(sb.toString());
			}
		} catch (MalformedURLException e) {
			// Invalid parameter
			logger.log(Level.SEVERE, e.toString(), e);
		} catch (ParseException e) {
			// could not parse connection response as JSON Object
			logger.log(Level.SEVERE, e.toString(), e);
		} catch (IOException e) {
			// could not establish connection
			logger.log(Level.SEVERE, e.toString(), e);
		}
		return null;
	}
	
}
