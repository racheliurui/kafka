package learn.rachel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Rachel on 11/18/2017.
 */
public class kafkaClient {

  static String bootstrap= "localhost:9092";
  //  static String bootstrap= "192.168.1.222:9092";
    public  static void main(String[] args){
   producer();
   consumer();

    }

public  static void consumer(){

    Properties props = new Properties();
    props.put("bootstrap.servers",bootstrap);
    props.put("group.id", "kafkaclient");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("foo", "bar"));
    System.out.printf("started");
    while (true) {
        System.out.println("pull");
        ConsumerRecords<String, String> records = consumer.poll(100);
        System.out.println(records.count());
        for (ConsumerRecord<String, String> record : records)
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        System.out.println("pulled");
    }
}

   public static void producer (){

       Properties props = new Properties();
       props.put("bootstrap.servers", bootstrap);
       props.put("acks", "all");
       props.put("retries", 0);
       props.put("batch.size", 16384);
       props.put("linger.ms", 1);
       props.put("buffer.memory", 33554432);
       props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

       Producer<String, String> producer = new KafkaProducer<>(props);
       for (int i = 0; i < 100; i++)
           producer.send(new ProducerRecord<String, String>("foo", Integer.toString(i), Integer.toString(i)));

       producer.close();

   }
}
