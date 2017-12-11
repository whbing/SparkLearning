package cn.whbing.spark.SparkApps.cores;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopNBasic {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("SecondSort").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("OFF");
		JavaRDD<String> lines = sc.textFile("D://javaTools//EclipseWork2//SparkApps//topNBasic.txt");

		//将line转化为(Integer,line)
		JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(String line) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(line), line);
			}
		});
		JavaPairRDD<Integer, String>  sortedPairs = pairs.sortByKey(false);//Integer,String型已经默认可以排序
		List<Tuple2<Integer, String>> topN = sortedPairs.take(5);
		for(Tuple2<Integer, String> perTopN : topN){
			System.out.println(perTopN._2);
		}
		
	}
}
