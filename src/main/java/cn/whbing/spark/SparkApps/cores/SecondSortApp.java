package cn.whbing.spark.SparkApps.cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/*
 * 二次排序：
 * 第一步：按照Ordered和serializable接口实现自定义排序
 * 第二步：将要排序的二次排序的文件加载进<Key, Value>类型的RDD
 * 第三步：使用sortByKey基于自定义的Key进行二次排序
 * 第四步：去除掉排序的Key，只保留排序后的结果
 * 
 */
public class SecondSortApp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("SecondSort").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		JavaRDD<String> lines = sc.textFile("D://javaTools//EclipseWork2//SparkApps//SecondSort.txt");
		JavaPairRDD<SecondSortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondSortKey, String>() {
			//String:lines读进来的内容， K2：处理的key，为SecondSortKey，V2：String

			@Override
			public Tuple2<SecondSortKey, String> call(String line) throws Exception {
				String[] splited = line.split(" ");
				SecondSortKey key = new SecondSortKey(
						Integer.valueOf(splited[0]), Integer.valueOf(splited[1]));
				return new Tuple2(key, line);
			}			
		});
		JavaPairRDD<SecondSortKey, String> sorted = pairs.sortByKey();//完成二次排序
		//过滤掉排序后的key，保留原结果
		JavaRDD<String> secondSorted = sorted.map(new Function<Tuple2<SecondSortKey,String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<SecondSortKey, String> sortedContent) throws Exception {
				
				return sortedContent._2;
			}
		});
		
		secondSorted.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String sorted) throws Exception {
				System.out.println(sorted);
			}
		});
	}
}
