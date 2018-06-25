package main.java.pw.oliver.jmkb;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

public class ConfigurationFileReader {
	
	private Properties config;
	
	public ConfigurationFileReader() {
		config = new Properties();
		try {
			FileInputStream fis = new FileInputStream("./jmkb.properties");
			config.load(fis);
			fis.close();
			if (!config.containsKey("frostServerURI")
					|| !config.containsKey("kafkaBrokerURI")
					|| !config.containsKey("schemaRegistryURI")
					|| !config.containsKey("schemaId")) {
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
		return config.getProperty(key);
	}
}
