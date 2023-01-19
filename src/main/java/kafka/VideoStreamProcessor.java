package kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Base64;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.opencv.core.Mat;
import org.opencv.imgcodecs.Imgcodecs;
public class VideoStreamProcessor {
    private static final Logger logger = Logger.getLogger(VideoStreamProcessor.class);

    private static Mat getMat(VideoEventData ed) throws Exception{
        Mat mat = new Mat(ed.getRows(), ed.getCols(), ed.getType());
        mat.put(0, 0, Base64.getDecoder().decode(ed.getData()));
        return mat;
    }

    //Save image file
    private static void saveImage(Mat mat,VideoEventData ed,String outputDir) {
        String imagePath = outputDir + ed.getCameraId() + "-T-" + ed.getTimestamp().getTime() + ".png";
        logger.warn("Saving images to " + imagePath);
        boolean result = Imgcodecs.imwrite(imagePath, mat);
        if (!result) {
            logger.error("Couldn't save images to path " + outputDir + ".Please check if this path exists. This is configured in processed.output.dir key of property file.");
        }
    }
    public static void main(String[] args) throws Exception{
        // Getting properties from file

        try (InputStream input = VideoStreamCollector.class.getClassLoader().getResourceAsStream("kafka.properties")) {

            Properties prop = new Properties();
            prop.load(input);
            // SparkSesion
            SparkSession spark = SparkSession
                    .builder()
                    .appName("VideoStreamProcessor")
                    .master(prop.getProperty("spark.master.url"))
                    .getOrCreate();

        //directory to save image files with motion detected
        final String processedImageDir = prop.getProperty("processed.output.dir");
        logger.warn("Output directory for saving processed images is set to "+processedImageDir+". This is configured in processed.output.dir key of property file.");

            //create schema for json message
        StructType schema =  DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("cameraId", DataTypes.StringType, true),
                DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
                DataTypes.createStructField("rows", DataTypes.IntegerType, true),
                DataTypes.createStructField("cols", DataTypes.IntegerType, true),
                DataTypes.createStructField("type", DataTypes.IntegerType, true),
                DataTypes.createStructField("data", DataTypes.StringType, true)
            });

        //Create DataSet from stream messages from kafka
        Dataset<VideoEventData> ds = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
                    .option("subscribe", prop.getProperty("kafka.topic"))
                    .option("kafka.max.partition.fetch.bytes", prop.getProperty("kafka.max.partition.fetch.bytes"))
                    .option("kafka.max.poll.records", prop.getProperty("kafka.max.poll.records"))
                    .load()
                    .selectExpr("CAST(value AS STRING) as message")     // convert the binary value to String
                    .select(functions.from_json(functions.col("message"),schema).as("json"))        // extract the value which is in JSON String to DataFrame and convert to DataFrame columns using custom schema.
                    .select("json.*")
                    .as(Encoders.bean(VideoEventData.class));

//        System.out.println("Message from Broker "+ds);
        ds.printSchema();   // verify to catch stream from the topic
        ds.
        //key-value pair of cameraId-VideoEventData
        KeyValueGroupedDataset<String, VideoEventData> kvDataset = ds.groupByKey(new MapFunction<VideoEventData, String>() {
        @Override
        public String call(VideoEventData value) throws Exception {
            return value.getCameraId();
        }
        }, Encoders.STRING());
        // process
        kvDataset.keys();
        Dataset<VideoEventData> processedDataset = kvDataset.mapGroupsWithState(new MapGroupsWithStateFunction<String, VideoEventData, VideoEventData,VideoEventData>(){
            @Override
            public VideoEventData call(String key, Iterator<VideoEventData> values, GroupState<VideoEventData> state) throws Exception {
                logger.warn("CameraId="+key+" PartitionId="+TaskContext.getPartitionId());
                VideoEventData existing = null;
                //check previous state
                if (state.exists()) {
                    existing = state.get();

                }
                System.out.println("existing"+existing);
                //detect motion
                VideoEventData processed = VideoProcessing.takeImage(key,values,processedImageDir,existing);

                //update last processed
                if(processed != null){
                    state.update(processed);
                }
                return processed;
            }}, Encoders.bean(VideoEventData.class), Encoders.bean(VideoEventData.class));


        //start
        StreamingQuery query = processedDataset.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        //await
        query.awaitTermination();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}


