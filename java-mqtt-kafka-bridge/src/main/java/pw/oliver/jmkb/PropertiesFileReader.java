package main.java.pw.oliver.jmkb;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

public class PropertiesFileReader {

	private Properties properties;
	
	public PropertiesFileReader() {
		properties = new Properties();
		try {
			FileInputStream fis = new FileInputStream("./jmkb.properties");
			properties.load(fis);
			fis.close();
			if (!properties.containsKey("frostServerURI")
					|| !properties.containsKey("kafkaBrokerURI")
					|| !properties.containsKey("schemaRegistryURI")
					|| !properties.containsKey("schemaId")) {
				throw new InvalidParameterException();
			}
		} catch (InvalidParameterException e) {
			e.printStackTrace();
			System.err.println("The configuration file is missing at least one of the following required arguments:\n"
					+ "\t- frostServerURI\n"
					+ "\t- kafkaBrokerURI\n"
					+ "\t- schemaRegistryURI\n"
					+ "\t- schemaId\n");
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("There was an error reading the configuration file.\n"
					+ "Please make sure that there is a file named 'jmkb.properties' at the root directory of the program.");
			System.exit(-1);
		}
	}
	
	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
