package cn.whbing.spark.SparkApps.sql;


import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;

/*
 * 实战dataset<row>
 */
public class DataFrameOps {

	public static void main(String[] args) throws AnalysisException {
		
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("spark SQL")
				.getOrCreate();
				
		//Dataset<Row> df = spark.read().json("hdfs://master-1a:9000/whbing/data/nvzhuang.json");
		Dataset<Row> df = spark.read().json("D:\\javaTools\\EclipseWork1\\taobaospider\\nvzhuang.json");
		
		//select * from table
		//df.show();

		
		//describe table;
		//df.printSchema();
		
		// select price from table;
		//df.select("price").show();
		
		// select store price-1000 from table;
		//df.select(col("store"), col("price").plus(-1000)).show();
		
		//select * from table where price > 500;
		//df.filter(col("price").gt(500)).show();
		
		//count items by class2
		//select count(1) from table group by class2;
		//df.groupBy("class2").count().show();
		
		/***************************************/
		
		//将dataframe注册为临时视图
		df.createOrReplaceTempView("nvzhuang");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM nvzhuang");
		//sqlDF.show();
		
		//将dataframe注册为全局临时视图
		df.createGlobalTempView("nvzhuang1");
		//spark.sql("SELECT * FROM global_temp.nvzhuang1").show();
		//spark.newSession().sql("SELECT * FROM global_temp.nvzhuang1").show();
	}


}
