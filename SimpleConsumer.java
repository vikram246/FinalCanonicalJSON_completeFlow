package demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import Maven_test.XMLCreator;

public class SimpleConsumer {
  public static void main(String[] args) {
    Properties props = new Properties();
    
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"1");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    Object accId = 21;

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    try {
      // List of topics to subscribe to
      consumer.subscribe(Arrays.asList("avro"));
      while (true) {
        @SuppressWarnings("deprecation")
		ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
        	String value = record.value();
        	
        	
        	XMLCreator createXML = new XMLCreator();
			createXML.createXML(value);

        	
        
          System.out.printf("value = %s%n", record.value());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }
}