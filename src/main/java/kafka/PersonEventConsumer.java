package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PersonEventConsumer {
    public static void main(String[] args) throws UnknownHostException {
        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "192.168.1.200:9092,192.168.1.200:9093");
        config.put("group.id", "java-person-events");
        config.put("acks", "all");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(config);
        kafkaConsumer.subscribe(Arrays.asList("person_events"));
        System.out.println("starting consumer");
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            System.out.println(records.count());
            kafkaConsumer.commitSync();
        }
    }
}
