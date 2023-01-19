package kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

public class producer1 {
    public static void main(String args[])  throws Exception
    {
        // Getting properties from file

        try (InputStream input = producer1.class.getClassLoader().getResourceAsStream("kafka.properties")) {

            Properties prop = new Properties();
            prop.load(input);
            // load a properties file
            Properties properties = new Properties();
            properties.put("bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"));
            properties.put("acks", prop.getProperty("kafka.acks"));
            properties.put("retries", prop.getProperty("kafka.retries"));
            properties.put("batch.size", prop.getProperty("kafka.batch.size"));
            properties.put("linger.ms", prop.getProperty("kafka.linger.ms"));
            properties.put("max.request.size", prop.getProperty("kafka.max.request.size"));
            properties.put("compression.type", prop.getProperty("kafka.compression.type"));
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            // Creating producer
            KafkaProducer<String, String> first_producer = new KafkaProducer<String, String>(properties);

            // Creating record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(prop.getProperty("kafka.topic"), "Hello Kafka w properties file");    //my_first: topic name; Hello Kafka: message

            // Sending record
            first_producer.send(record);
            first_producer.flush();
            first_producer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}