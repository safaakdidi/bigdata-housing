package tn.insat.housing.streaming;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.function.* ;
import java.util.*;
public class RealTimeMonitoring {
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: RealTimeMonitoring <bootstrapServers> <topics>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];

        // Create a Spark configuration
        SparkConf conf = new SparkConf().setAppName("RealTimeMonitoring");

        // Create a Streaming Context with a batch interval of 1 second
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

        // Set the Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "RealTimeMonitoring");

        // Create a list of topics to subscribe to
        Collection<String> topicsSet = Arrays.asList(topics.split(","));

        // Create a DStream from Kafka
        JavaPairDStream<String, String> housingListings = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        ).mapToPair(new Funct record -> new Tuple2<>(record.key().toString(), record.value().toString()));


        // Print the new housing listings
        housingListings.foreachRDD(rdd -> {
            if (rdd.count() > 0) {
                System.out.println("New housing listings:");
                rdd.foreach(record -> System.out.println(record.value()));
            }
        });

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}
