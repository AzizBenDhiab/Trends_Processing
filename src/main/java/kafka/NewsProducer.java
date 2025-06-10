package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class NewsProducer {
    public static void main(String[] args) {
        String topic = "news"; // Same topic as before

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.12:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String newsJson = NewsApiFetcher.fetchNewsJson();
            producer.send(new ProducerRecord<>(topic, newsJson));

            System.out.println("News JSON sent to Kafka topic."+ newsJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
