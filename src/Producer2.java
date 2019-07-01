import java.util.*;
import java.util.concurrent.ExecutionException;

import javax.security.auth.callback.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.*;

public class Producer2 {
	@SuppressWarnings("resource")
	public static void main(String[] args)throws InterruptedException,ExecutionException {
		final String topic = "test_topic";
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost.localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName() );
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);
		System.out.println("Sends 1 message every second for a total of 50 seconds");
		for(int i =0;i<50;++i) {
			String message = "Message Number : "+i+" at "+ new Date();
			String key = "key"+i;
			ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,message);
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata,Exception exception) {
					if(exception == null) {
						System.out.println("Message delivered successfully : "+message);
					}
					else {
						System.out.println("Message not delivered : "+exception.getMessage());
					}
				}
			});
			System.out.println("Message sent : "+message);
			Thread.sleep(1000);
			
		}
	}
}
