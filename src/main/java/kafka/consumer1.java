package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;




public class consumer1 {
    public static void main(String[] args){
        // Creating logger
        Logger logger = LoggerFactory.getLogger(consumer1.class.getName());

        // Creating consumer properties
        String bootstrapServers = "127.0.0.1:9092";
        String group_id = "debug";
        String topic = "video-stream-event";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Creating consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);

        // Subscribing
        consumer.subscribe(Arrays.asList(topic));
//        consumer.subscribe(topic);
        // Polling
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record: records){
                System.out.println("Key: "+record.key() + ";" + "Value: "+record.value());
//                logger.info("Key: "+record.key() + ";" + "Value: "+record.value());
//                logger.info("Partition: "+record.partition()+ ";" + "Offset :"+record.offset());
            }
        }
    }
}
