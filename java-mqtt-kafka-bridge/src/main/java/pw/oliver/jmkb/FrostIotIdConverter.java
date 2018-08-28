package pw.oliver.jmkb;

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

/**
 * Helper class to extract a single or multiple {@code @iot.id}s from FROST's {@code @iot.navigationLinks}. 
 * @author Oliver
 *
 */
public class FrostIotIdConverter {

	private static final Logger LOGGER = Logger.getLogger(FrostIotIdConverter.class.getName());
	
	/**
	 * Default constructor.
	 */
	public FrostIotIdConverter() {
		
	}
	
	/**
	 * Get a single or multiple {@code @iot.id}s from a given link.
	 * @param link The address to poll from
	 * @return A String containing a single {@code @iot.id} or a commaseparated
	 * enumeration of multiple {@code @iot.id}s.
	 */
	public String getIotIds(String link) {
		if (link == null) {
			return null;
		}
		JSONObject jo = getJSONObjectFromNavigationLink(link);
		if (jo.containsKey("@iot.id")) {
			return jo.get("@iot.id").toString();
		} else if (jo.containsKey("value")) {
			LinkedList<String> ll = new LinkedList<>();
			JSONArray ja = (JSONArray) jo.get("value");
			Iterator<?> it = ja.iterator();
			while (it.hasNext()) {
				ll.add(((JSONObject) it.next()).get("@iot.id").toString());
			}
			String ids = String.join(",", ll);
			return ids;
		} else {
			return null;
		}
	}
	
	/**
	 * @deprecated
	 * Get a single iot.id from given link.
	 * DEPRECATED: Please use new method that does not differentiate between single and multiple iot.ids.
	 * @param link The address to poll from
	 * @return A String containing a single iot.id
	 */
	public String getSingleIotId(String link) {
		if (link == null) {
			return null;
		}
		return getJSONObjectFromNavigationLink(link).get("@iot.id").toString();
	}

	/**
	 * @deprecated
	 * Get a comma separated enumeration of multiple iot.ids from given link.
	 * DEPRECATED: Please use new method that does not differentiate between single and multiple iot.ids.
	 * @param link The address to poll from
	 * @return A String containing a comma separated list of multiple iot.ids
	 */
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
			LOGGER.log(Level.SEVERE, e.toString(), e);
		} catch (ParseException e) {
			// could not parse connection response as JSON Object
			LOGGER.log(Level.SEVERE, e.toString(), e);
		} catch (IOException e) {
			// could not establish connection
			LOGGER.log(Level.SEVERE, e.toString(), e);
		}
		return null;
	}
	
}
