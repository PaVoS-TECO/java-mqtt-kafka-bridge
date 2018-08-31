package pw.oliver.jmkb;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MqttMessageConverterTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		PropertiesFileReader.init();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testConstructor() {
		assertNotNull(new MqttMessageConverter());
	}

	@Test
	public void testMqttMessageToDatastream() {
		MqttMessageConverter conv = new MqttMessageConverter();
	}

}
