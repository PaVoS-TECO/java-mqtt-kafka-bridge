package pw.oliver.jmkb;

import static org.junit.Assert.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropertiesFileReaderTest {
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		Paths.get("jmkb.properties").toAbsolutePath().toFile().delete();
		Properties properties = new Properties();
		properties.setProperty("frostServerURI", "tcp://127.0.0.1:1883");
		properties.setProperty("kafkaBrokerURI", "http://127.0.0.1:9092");
		properties.setProperty("schemaRegistryURI", "http://127.0.0.1:8081");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.flush();
		fos.close();
	}
	
	@Test
	public void testValidPropertiesTwiceAndGetProp() throws IOException {
		Paths.get("jmkb.properties").toAbsolutePath().toFile().delete();
		Properties properties = new Properties();
		properties.setProperty("frostServerURI", "tcp://127.0.0.1:1883");
		properties.setProperty("kafkaBrokerURI", "http://127.0.0.1:9092");
		properties.setProperty("schemaRegistryURI", "http://127.0.0.1:8081");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.flush();
		fos.close();
		PropertiesFileReader.init();
		PropertiesFileReader.init();
		PropertiesFileReader.getProperty("frostServerURI");
	}

	@Test
	// incomplete properties list
	public void testInvalidProperties1() throws IOException {
		Paths.get("jmkb.properties").toAbsolutePath().toFile().delete();
		Properties properties = new Properties();
		properties.setProperty("frostServerURI", "127.0.0.1");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.flush();
		fos.close();
		PropertiesFileReader.init();
	}
	
	@Test
	// test no such file found
	public void testInvalidProperties2() throws IOException {
		Paths.get("jmkb.properties").toAbsolutePath().toFile().delete();
		PropertiesFileReader.init();
	}
	
	@Test
	// test bad format
	public void testInvalidProperties3() throws IOException {
		Paths.get("jmkb.properties").toAbsolutePath().toFile().delete();
		Properties properties = new Properties();
		properties.setProperty("frostServerURI", "127.0.0.1");
		properties.setProperty("kafkaBrokerURI", "127.0.0.1");
		properties.setProperty("schemaRegistryURI", "127.0.0.1");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.flush();
		fos.close();
		PropertiesFileReader.init();
	}
	
	@Test
	// test invalid URI
	public void testInvalidProperties4() throws IOException {
		Paths.get("jmkb.properties").toAbsolutePath().toFile().delete();
		Properties properties = new Properties();
		properties.setProperty("frostServerURI", "300.0|.256.137");
		properties.setProperty("kafkaBrokerURI", "300.0|.256.137");
		properties.setProperty("schemaRegistryURI", "300.0|.256.137");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.flush();
		fos.close();
		PropertiesFileReader.init();
	}
	
}
