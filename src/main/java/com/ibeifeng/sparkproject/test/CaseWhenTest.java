package com.ibeifeng.sparkproject.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangxueqiang
 * @create 2018-11-08 13:27
 */
public class CaseWhenTest {
	public  static  void  main(String[] args){
		//构建spark的应用上下文
		SparkConf conf = new SparkConf().setMaster("local").setAppName("MyCaseWhenTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//创建spak-sql
		SQLContext sqlContext = new SQLContext(sc.sc());
		//处理数据
		List<Integer> grades = Arrays.asList(99,100,58,70,80);
		//gradesRDD
		JavaRDD<Integer> gradesRDD = sc.parallelize(grades);
		//gradeRowsRDD
		JavaRDD<Row> gradeRowsRDD = gradesRDD.map(new Function<Integer, Row>() {
			@Override
			public Row call(Integer grade) throws Exception {
				return RowFactory.create(grade);
			}
		});
		//临时表结构
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("grade", DataTypes.IntegerType, true)));
		//加载数据到临时表
		DataFrame gradesDF = sqlContext.createDataFrame(gradeRowsRDD,schema);
		//临时表表名
		gradesDF.registerTempTable("grades");
		//查询语句
		DataFrame gradeLevelDF = sqlContext.sql(
				"SELECT CASE "
						+ "WHEN grade>=90 THEN 'A' "
						+ "WHEN grade>=80 THEN 'B' "
						+ "WHEN grade>=70 THEN 'C' "
						+ "WHEN grade>=60 THEN 'D' "
						+ "ELSE 'E' "
						+ "END gradeLevel "
						+ "FROM grades");

		gradeLevelDF.show();
		sc.close();
	}
}
