package cn.whbing.spark.SparkApps.cores;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/*
 * 分组topN：
 * 第一步：先分组
 * 第二步：在每个组中获得前N个
 */
public class TopNGroup {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("SecondSort").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		JavaRDD<String> lines = sc.textFile("D://javaTools//EclipseWork2//SparkApps//topNGroup.txt");
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String,Integer>() {
			//String:lines读进来的内容，

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] splited = line.split(" ");
				return new Tuple2(splited[0], Integer.valueOf(splited[1]));
			}			
		});
		JavaPairRDD<String,Iterable<Integer>> grouped = pairs.groupByKey();
		//groupByKey的key为原来的key，value为原来的value的集合
		
		//选取value中的前N个
		JavaPairRDD<String,Iterable<Integer>> top5 = grouped.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {
					//传进来的是Tuple2<String,Iterable<Integer>>
					//返回的仍然是该类型，只是iterable进行处理了
					@Override
					public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)
							throws Exception {
						String key = t._1;
						Integer[] value = new Integer[5];//用于存放top5
						Iterator it = t._2.iterator();
						while(it.hasNext()){
							Integer top = (int)it.next();
							for(int i=0;i<5;i++){
								if(value[i]==null){
									value[i]=top;
									break;
								}else if( top > value[i]){
									//第i个及以后的数据往后移
									for(int j=4;j>i;j--){
										value[j]=value[j-1];
									}
									value[i] = top;
									break;
								}
							}
						}
						return new Tuple2<String, Iterable<Integer>>(key, Arrays.asList(value));
					}
		});
		//打印出来
		top5.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println("group by key and the top5,key:"+t._1);
				System.out.println(t._2);
			}
		});

	}
}
