import java.util.*;
import javax.security.auth.callback.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyKafkaProducer {

   public static void main(String[] args) throws InterruptedException,ExecutionException {
         final String topic = "test_topic";
         Properties props = new Properties();
	  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost.localdomain:9092");         
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,    
                                                      StringSerializer.class.getName());
	  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,                  
                                                      StringSerializer.class.getName());
          
          @SuppressWarnings("resource")
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);
          System.out.println("Sending 1 message every 1 second, for a total of 100 messages:");
            
          for(int i=0; i <100; i++) {   
              String message = "Message Number: " + i + " at " + new Date();
              String key = "key" + i;
              ProducerRecord<String,String> record = new ProducerRecord  
                                                     <String,String>(topic, key, message);
                
              producer.send(record, new Callback() {
		    public void onCompletion(RecordMetadata metadata, Exception exception) {
		                if (exception == null) {
		                    System.out.println("Message Delivered Successfully: " + 
                                                                                 message);
		                    } else {
		                      System.out.println("Message Not Delivered : " + 
                                          message + ". Cause: " + exception.getMessage());
		                    } //end of else
		   } //end of onCompletion
	      }); //end of producer call
              System.out.println("Message Sent:" + message);
              Thread.sleep(1000);
          } //end of for loop
       
   } //end of main method
} //end of class
