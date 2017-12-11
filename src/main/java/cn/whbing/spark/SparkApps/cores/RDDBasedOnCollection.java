package cn.whbing.spark.SparkApps.cores;

/**
 * @author whbing1991@gmail.com
 * 利用jiva集合创建RDD的示例
 */
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


public class RDDBasedOnCollection {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("RDD based on javaCollection").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(int i=0;i<100;i++){
			list.add(i+1);
		}
	
		JavaRDD<Integer> rdd =  sc.parallelize(list);
		Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		System.out.println(sum);
	}

}
