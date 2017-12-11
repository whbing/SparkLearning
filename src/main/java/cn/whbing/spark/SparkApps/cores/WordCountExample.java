package cn.whbing.spark.SparkApps.cores;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 官方示例版本
 */
import scala.Tuple2;

public class WordCountExample {
public static void main(String[] args) {		
		 
		SparkConf conf = new SparkConf().setAppName("Spark WordCount written by java!");

		JavaSparkContext sc = new JavaSparkContext(conf);
		

		JavaRDD<String> textFile = sc.textFile("hdfs:///whbing/HelloSpark.txt");

		JavaPairRDD<String, Integer> counts = textFile
			    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			    .mapToPair(word -> new Tuple2<>(word, 1))
			    .reduceByKey((a, b) -> a + b);
		
		counts.saveAsTextFile("hdfs:///home/whbing/HelloSpark2");
		sc.close();
		
	}
}
