
## Creating Kafka Producer

Basically, there are four steps to create a java producer, as discussed earlier:
1. Create producer properties
2. Create the producer
3. Create a producer record
4. Send data
5. Check the output in the terminal of broker

```
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic my_first --group first_app
```

## Creating Kafka Consumer

There are following steps taken to create a consumer:
1. Create Logger
2. Create consumer properties
3. Create a consumer
4. Subscribe the consumer to a specific topic
5. Poll for some new data
6. Check the output, have to run by the order: ```consumer -> producer```

## Developing Video Stream Collector
The video stream collector works with a cluster of IP cameras that provide live video feeds. 
The component must read the feed from each camera and convert the video stream into a series of video frames. 
To distinguish each IP camera, the collector maintains the mapping of camera ID and URL with ```camera.url``` and ```camera.id``` properties in a `.properties` file. 
These properties can have comma-separated lists of camera URLs and IDs. 
Different cameras may provide data with different specifications such as the codec, resolution, or frames per second. 
The collector must retain these details while creating frames from the video stream.
OpenCV stores each frame or image as a `Mat object`. Mat needs to be converted in serialise-able (byte-array) form by keeping intact the details of frame - i.e. rows, columns, and type. 
The video stream collector uses the following JSON message structure to store these details. Then, the image will be is encoded under base-64 string.
The video stream collector uses the `Gson` library to convert the data to JSON messages, which are published  in the video-stream-event topic. 
It sends the JSON messages to the `Kafka` broker using the `KafkaProducer` client. KafkaProducer sends data into the same partition for each key and order of these messages is guaranteed.

## Stream Data Buffer
To process a huge amount of video stream data without loss, it is necessary to store the stream data in temporary storage. 
The Kafka broker works as a buffer queue for the data that the video stream collector produces. 
Kafka uses the file system to store the messages, and the length of time it retains these messages is configurable.
Kafka guarantees the order of messages in a single partition for a given topic. This is extremely helpful for processing data when the order of the data is important. 

## Developing Video Stream Processor
The video stream processor performs three steps:
1. Read the JSON messages from the Kafka broker in the form of a VideoEventData dataset.
2. Group the VideoEventData dataset by camera ID and pass it to the video stream processor.
3. Create a Mat object from the JSON data and process the video stream data.

***Note***: At consumer side, there are two options:
1. Using KafkaConsumer, which reads the data from topic.
2. Consume kafkaTopic Data using Spark Stream.

The major difference: a Spark cluster to run Spark code in a distributed fashion compared to the Kafka Consumer just runs in a single JVM and you run multiple instances of the same application manually to scale it out.
