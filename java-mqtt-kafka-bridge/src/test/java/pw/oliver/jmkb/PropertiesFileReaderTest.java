package pw.oliver.jmkb;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropertiesFileReaderTest {

	private static Properties propertiesOrig;
	private static Properties properties;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		propertiesOrig = new Properties();
		FileInputStream fis = new FileInputStream("./jmkb.properties");
		propertiesOrig.load(fis);
		fis.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		propertiesOrig.store(fos, null);
		fos.close();
	}
	
	@Test
	public void testValidPropertiesTwice() throws IOException {
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		propertiesOrig.store(fos, null);
		fos.close();
		PropertiesFileReader.init();
		PropertiesFileReader.init();
	}

	@Test
	// incomplete properties list
	public void testInvalidProperties1() throws IOException {
		properties = new Properties();
		properties.setProperty("frostServerURI", "127.0.0.1");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.close();
		PropertiesFileReader.init();
	}
	
	@Test
	// test no such file found
	public void testInvalidProperties2() throws IOException {
		new File("./jmkb.properties").delete();
		PropertiesFileReader.init();
	}
	
	@Test
	// test bad format
	public void testInvalidProperties3() throws IOException {
		properties = new Properties();
		properties.setProperty("frostServerURI", "127.0.0.1");
		properties.setProperty("kafkaBrokerURI", "127.0.0.1");
		properties.setProperty("schemaRegistryURI", "127.0.0.1");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.close();
		PropertiesFileReader.init();
	}
	
	@Test
	// test invalid URI
	public void testInvalidProperties4() throws IOException {
		properties = new Properties();
		properties.setProperty("frostServerURI", "lorem");
		properties.setProperty("kafkaBrokerURI", "ipsum");
		properties.setProperty("schemaRegistryURI", "dolor");
		FileOutputStream fos = new FileOutputStream("./jmkb.properties");
		properties.store(fos, null);
		fos.close();
		PropertiesFileReader.init();
	}
	
	@Test
	public void 

}
