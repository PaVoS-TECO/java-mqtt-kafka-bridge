package pw.oliver.jmkb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to extract a single or multiple {@code @iot.id}s from FROST's {@code @iot.navigationLinks}. 
 * @author Oliver
 *
 */
public class FrostIotIdConverter {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Get a single or multiple {@code @iot.id}s from a given link.
	 * @param link The address to poll from
	 * @return A String containing a single {@code @iot.id} or a comma separated
	 * enumeration of multiple {@code @iot.id}s.
	 */
	public String getIotIds(String link) {
		if (link == null) {
			return null;
		}
		JSONObject jo = getJSONObjectFromNavigationLink(link);
		if (jo == null) {
			return null;
		}
		if (jo.containsKey("@iot.id")) {
			// only a single @iot.id available
			return jo.get("@iot.id").toString();
		} else if (jo.containsKey("value")) {
			// multiple @iot.ids available
			LinkedList<String> ll = new LinkedList<>();
			JSONArray ja = (JSONArray) jo.get("value");
			Iterator<?> it = ja.iterator();
			while (it.hasNext()) {
				ll.add(((JSONObject) it.next()).get("@iot.id").toString());
			}
			return String.join(",", ll);
		} else {
			return null;
		}
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
			logger.warn("Invalid @iot.navigationLink {}", link, e);
		} catch (ParseException e) {
			// could not parse connection response as JSON Object
			logger.warn("Could not parse response of {} as JSONObject", link, e);
		} catch (IOException e) {
			// could not establish connection
			logger.warn("Could not establish connection to {}", link, e);
		}
		return null;
	}
	
}
