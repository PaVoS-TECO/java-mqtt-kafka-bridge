package pw.oliver.jmkb;

import static org.junit.Assert.*;

import org.apache.http.ConnectionClosedException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JmkbMqttConsumerTest {

	private static JmkbMqttConsumer cons;
	private static JmkbKafkaProducer prod;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		PropertiesFileReader.init();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testConstructor() {
		prod = new JmkbKafkaProducer();
		cons = new JmkbMqttConsumer("testConsumer", prod);
		assertNotNull(cons);
		prod.disconnect();
		cons.disconnect();
	}
	
	@Test
	public void testConnectionLost() {
		prod = new JmkbKafkaProducer();
		cons = new JmkbMqttConsumer("testConsumer", prod);
		cons.connectionLost(null);
		prod.disconnect();
		cons.disconnect();
	}
	
	@Test
	public void testDeliveryComplete() {
		prod = new JmkbKafkaProducer();
		cons = new JmkbMqttConsumer("testConsumer", prod);
		cons.deliveryComplete(null);
		prod.disconnect();
		cons.disconnect();
	}
	
	@Test
	public void testDisconnect() {
		prod = new JmkbKafkaProducer();
		cons = new JmkbMqttConsumer("testDisconnect", prod);
		prod.disconnect();
		cons.disconnect();
	}

}
