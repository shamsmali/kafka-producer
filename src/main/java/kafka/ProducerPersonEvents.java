package kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerPersonEvents {
  public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException {
    Properties config = new Properties();
    config.put("client.id", InetAddress.getLocalHost().getHostName());
    config.put("bootstrap.servers", "192.168.0.76:9092,192.168.0.76:9093");
    config.put("acks", "all");
    config.put("key.serializer", StringSerializer.class.getName());
    config.put("value.serializer", StringSerializer.class.getName());

    KafkaProducer kafkaProducer = new KafkaProducer<String, String>(config);

    while (true) {
      Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord("person_events", "shams mali" + System.currentTimeMillis()));
      System.out.println("Sending message");
      RecordMetadata metadata = future.get();
      System.out.println(metadata);


      Thread.sleep(500);
    }
  }
}
