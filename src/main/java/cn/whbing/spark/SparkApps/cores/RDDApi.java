package cn.whbing.spark.SparkApps.cores;

/**
 * @author whbing1991@gmail.com
 * RDDAPI的学习
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import javassist.expr.Instanceof;
import scala.Tuple1;
import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;


public class RDDApi{
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("RDD API Test").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(int i=0;i<100;i++){
			list.add(i+1);
		}	
		JavaRDD<Integer> rdd =  sc.parallelize(list);
		
		/*1.map*/
		JavaRDD<Integer> rdd2 = rdd.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				return v1*2;
			}
		});
		System.out.println("1.map结果：key*2：");
		rdd2.foreach(new VoidFunction<Integer>() {			
			@Override
			public void call(Integer t) throws Exception {
				System.out.print(t+" ");				
			}
		});
		/*end map*/		
		/*2.filter*/
		JavaRDD<Integer> rdd3 = rdd.filter(new Function<Integer, Boolean>() {
			
			@Override
			public Boolean call(Integer v1) throws Exception {
				if(v1%3==0){
					return false;//过滤掉3的倍数
				}
				return true;
			}
		});
		System.out.println("2.filter结果：过滤3的倍数：");
		rdd3.foreach(new VoidFunction<Integer>() {			
			@Override
			public void call(Integer t) throws Exception {
				System.out.print(t+" ");				
			}
		});
		/*end filter*/
		/*3.flatMap*/
		List<String> list2 =  Arrays.asList("hello spark !","hello java","hello today");
		JavaRDD<String> rddFlatMap = sc.parallelize(list2);
		JavaRDD<String> rddFlatMap2 = rddFlatMap.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {

				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		System.out.println("3.flatMap原数据：");
		rddFlatMap.foreach(new VoidFunction<String>() {			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);				
			}
		});
		System.out.println("3.flatMap结果：对每个key以空格分开");
		rddFlatMap2.foreach(new VoidFunction<String>() {			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);				
			}
		});
		/*end flatMap*/
		/*4.groupByKey*/
		List scoreList = Arrays.asList(
				new Tuple2("class1", 80),
				new Tuple2("class2", 90),
				new Tuple2("class1",100),
				new Tuple2("class1",60)
		);
		JavaPairRDD rddPair = sc.parallelizePairs(scoreList);
		JavaPairRDD rddGroupByKey2 = rddPair.groupByKey();
		rddGroupByKey2.foreach(new VoidFunction<Tuple2>() {

			@Override
			public void call(Tuple2 t) throws Exception {
				System.out.println("4.groupbykey");
				System.out.println("class:"+t._1);
				System.out.println(t._2);							
			}
		});
		/*end groupbykey*/
		/*5.reduce by key:统计每个班级的总分*/
		JavaPairRDD<String, Integer> rddReduceByKey = rddPair.reduceByKey(new Function2<Integer, Integer,Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		}); 
		System.out.println("5.reduce by key:");
		rddReduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
		
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1);
				System.out.println(t._2);
			}
		});
		/*end reduce by key*/
		/*6.sort by key*/
		List li = Arrays.asList(
			new Tuple2<Integer,String>(99,"anni"),
			new Tuple2<Integer,String>(88,"tony"),
			new Tuple2<>(100, "whb"),
			new Tuple2<>(60, "miss")
		);
		JavaPairRDD<Integer, String> rddbysort = sc.parallelizePairs(li); 
		JavaPairRDD<Integer, String> rddBysort2 =rddbysort.sortByKey();//默认从小到大
		System.out.println("6.1.sort by key:默认按成绩从小到大排序的结果：");
		rddBysort2.foreach(new VoidFunction<Tuple2<Integer,String>>() {			
			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1);				
				System.out.println(t._2);				
			}
		});	
		/*7.join将学生id，姓名和id，成绩通过id关联起来*/
		List l1 = Arrays.asList(
			new Tuple2<Integer,String>(1,"anni"),
			new Tuple2<Integer,String>(2,"tony"),
			new Tuple2<Integer,String>(3,"ted"),
			new Tuple2<Integer,String>(4,"lucy")
		);
		
		List l2 = Arrays.asList(
			new Tuple2<Integer,Integer>(1,80),
			new Tuple2<Integer,Integer>(1,90),
			new Tuple2<Integer,Integer>(2,40),
			new Tuple2<Integer,Integer>(3,66),
			new Tuple2<Integer,Integer>(4,50)
		);
		JavaPairRDD<Integer,String> rddList1 = sc.parallelizePairs(l1);
		JavaPairRDD<Integer,Integer> rddList2 = sc.parallelizePairs(l2);
		JavaPairRDD<Integer, Tuple2<String, Integer>> rddjoin = rddList1.join(rddList2);
		System.out.println("7.join:");
		rddjoin.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
				System.out.println(t._1);
				System.out.println(t._2);
				System.out.println("id:"+t._1+",name:"+t._2._1+",score:"+t._2._2);
			}
		});
		/*end join*/
        // cogroup与join不同
        // 相当于是，一个key join上的所有value，都给放到一个Iterable里面去了 
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> rddcogroup = rddList1.cogroup(rddList2);
		System.out.println("8.cogroup:");
		rddcogroup.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {			
			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
				System.out.println("id:"+t._1);
				System.out.println("name:"+t._2._1);
				System.out.println("score:"+t._2._2);
			}
		});
	}
}

