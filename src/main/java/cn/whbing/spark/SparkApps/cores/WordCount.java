package cn.whbing.spark.SparkApps.cores;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 使用java开发spark的wordcount程序，并打印出来
 *  
 * @author whbing whbing1991@gmail.com
 *
 */

public class WordCount {

	public static void main(String[] args) {
		
		/**
		 * step1:创建spark的配置对象SparkConf，配置Saprk运行时的配置信息。
		 * setMaster设置Spark集群的master URL。若为"local"表本地
		 */
		 
		//SparkConf conf = new SparkConf().setAppName("Spark WordCount written by java!");
		SparkConf conf = new SparkConf().setAppName("Spark WordCount written by java").setMaster("local");
		
		/**
		 * step2:创建SparkContext对象
		 * SparkContext是spark程序的唯一入口。无论采用scala，java，python，R都必须有一个sparkContext
		 * SparkContext核心作用：初始化Spark程序运行时核心组件，包括DAGScheduler、TeskScheduler、SchedulerBackend
		 * 同时还会负责Spark程序往master中注册程序
		 * 
		 * SparkContext是Spark程序中最为重要的一个对象
		 * java中的源码实际上也是赢scala写的
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/**
		 * step3:根据具体的数据来源（HDFD、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD
		 * RDD的创建基本有三种方式：外部数据来源（如HDFS）、根据scala集合、由其他的RDD操作
		 * 数据会被RDD划分为一系列的partitions，分配到威哥partition的数据属于一个task的处理范畴
		 * 
		 */
		//JavaRDD<String> lines = sc.textFile("hdfs:///whbing/HelloSpark.txt");
		JavaRDD<String> lines = sc.textFile("D://javaTools//IdeaWork//SparkApps//HelloSpark.txt");
		
		
		/**
		 * step4:对初始的javaRDD进行transformation级别的处理，例如map，filter等高级函数的编程
		 * step4.1：将每一行字符串拆分成单个的单词
		 * 如果是scala，可以SAM转化，直接val word =line.flatMap{line=>line.split(" ")}
		 * 
		 */
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String line) throws Exception {
				
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		/**
		 * 
		 * step4.2:在单词拆分的基础上对每个单词实例计数为1，也就是word=>(word,1)
		 */
		JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		/**
		 * step4.3:在每个单词实例计数为1的基础上统计每个单词在文本中出现的总次数
		 */
		JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {

				return v1+v2;
			}
		});
		
		
		/**
		 * step4.4:
		 */
        List<Tuple2<String, Integer>> output = wordsCount.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        sc.stop();
        /*
		wordsCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> pairs) throws Exception {
				System.out.println(pairs._1 +":"+ pairs._2);
			}
		});
		
		sc.close();
		*/
	}

}
