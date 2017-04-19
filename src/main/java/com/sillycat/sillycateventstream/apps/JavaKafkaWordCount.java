package com.sillycat.sillycateventstream.apps;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <zkQuorum>
 * is a list of one or more zookeeper servers that make quorum <group> is the
 * name of kafka consumer group <topics> is a list of one or more kafka topics
 * to consume from <numThreads> is the number of threads the kafka consumer
 * should use
 *
 * To run this example: `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \ zoo03
 * my-consumer-group topic1,topic2 1`
 */
public class JavaKafkaWordCount implements Serializable{
	
	private static final long serialVersionUID = -4598672873749563084L;

	private static final Pattern SPACE = Pattern.compile(" ");

	private JavaKafkaWordCount() {
	}

	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount").setMaster("local[2]");
		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10 * 1000));

		int numThreads = 1;
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("test", numThreads);

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "fr-stage-consumer:2181",
				"spark-group", topicMap);

		JavaDStream<String> lines = messages.map( x->{
			String value = x._2;
			return value;
		});
		

		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);

		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
	}

}
