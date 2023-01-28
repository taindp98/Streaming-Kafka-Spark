package kafka;
import java.io.IOException;
import java.io.InputStream;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.util.Base64;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import scala.util.parsing.json.JSON;
import java.nio.file.Path;

public class VideoStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(VideoStreamConsumer.class.getName());

    //load OpenCV native lib
    static {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
    }

    private static void saveImage(Mat mat,JsonObject msg, String outputDir){
//        clean timestamp format

        String imagePath = outputDir + msg.get("cameraId").getAsString() + "-T-" + msg.get("timestamp").getAsString()+".png";

        logger.warn("Saving images to "+imagePath);
//        debug
//        Mat mr1 = mat.row(1);
//        System.out.println(mr1.dump());
        boolean result = Imgcodecs.imwrite(imagePath, mat);
        if(!result){
            logger.error("Couldn't save images to path "+outputDir+".Please check if this path exists. This is configured in processed.output.dir key of property file.");
        }

    }
    private static Mat getMat(JsonObject msg) throws Exception{
        Mat mat = new Mat(msg.get("rows").getAsInt(), msg.get("cols").getAsInt(), msg.get("type").getAsInt());
        mat.put(0, 0, Base64.getDecoder().decode(msg.get("data").getAsString()));
        return mat;
    }

    public static void main(String[] args) throws Exception {
        // Creating logger

        try (InputStream input = VideoStreamConsumer.class.getClassLoader().getResourceAsStream("kafka.properties")) {

            Properties prop = new Properties();
            prop.load(input);
            Properties properties = new Properties();
            properties.put("bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"));
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("group.id", prop.getProperty("kafka.group.id"));
            properties.put("auto.offset.reset", prop.getProperty("kafka.auto.offset.reset"));
            // Creating consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            final String saveImgDir = prop.getProperty("processed.output.dir");
            // Subscribing
            consumer.subscribe(Arrays.asList(prop.getProperty("kafka.topic")));

            // Polling
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    JsonObject msg = new Gson().fromJson(record.value(), JsonObject.class);
                    Mat img = getMat(msg);
                    saveImage(img, msg, saveImgDir);
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
