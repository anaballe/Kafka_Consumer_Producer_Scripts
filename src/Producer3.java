import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public class Producer3{
	@SuppressWarnings("resource")
	public static void main(String[] args)throws InterruptedException,ExecutionException, IOException{
		Properties props = new Properties();
		final String topic = "test_topic_2";
		InputStreamReader imp = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(imp);
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost.localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);
		System.out.println("Enter your messages from here now");
		int i =1;
		while(true) {
			String msg = br.readLine();
			String key = "key"+i;
			if(msg.equals(""))
				break;
			ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,msg);
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata,Exception exception) {
					if(exception == null) {
						System.out.println("Message delivered successfully : "+ msg);
					}
					else {
						System.out.println("Message not delivered : "+ exception.getMessage());
					}
				}
			});
			//System.out.println("Message sent : "+msg);
			System.out.println("Message sent : "+msg);
		}
		System.out.println("Re-run your code to again become producer client !!!");
	}
}