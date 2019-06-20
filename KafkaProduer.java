package Maven_test;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProduer {

	public void producer(String val) {
		

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		String key = "Key1";
		String value = val;
		String topicName = "avro";
		// int partition = 0;
		System.out.println(val);

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		producer.send(record);
		producer.close();
		System.out.println("############### Canonical JSON pushed to topic successfully  ################");

	}

}
