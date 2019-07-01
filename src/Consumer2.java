import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.KafkaException;
public class Consumer2{
	public static void main(String[] args)throws InterruptedException,ExecutionException {
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("group.id","mygroup");
		props.put("enable.auto.commit","true");
		props.put("auto.commit.interval.ms","1000");
		props.put("session.timeout.ms","30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
		consumer.subscribe(Arrays.asList("test_topic"));
		try {
			while(true) {
			ConsumerRecords<String,String> records = consumer.poll(100);
			for(ConsumerRecord<String,String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s  ",record.offset(),record.key(),record.value());
				// print offset,key and value
				System.out.println();
			}
			}
		}
		catch(KafkaException exception) {
			System.out.println("There was an exception : " + exception);
			System.out.println();
		}
		finally {
			consumer.close();
		}
	}
}