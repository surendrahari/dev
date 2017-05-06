package org.big.data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 */
public class App {

    public static void main3 (String[] args) {
        SparkConf conf = new SparkConf().setAppName("myapp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        HiveContext hiveContext = new HiveContext(sc);
        //
        DataFrame df;
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("no", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("sal", DataTypes.IntegerType, false));
        StructType schema = DataTypes.createStructType(fields);
        df = sqlContext.read().format("com.databricks.spark.csv").schema(schema).option("header", "true")
                .option("inferSchema", "true").load("data.csv");
        //df.printSchema();
        //df.show();

        //df.selectExpr(new String[]{"sal"}).filter("sal > 2000").show();

        //df.select("name", "sal").filter("sal > 1500").show();

        //comare show vs sort with partitions
        //df.show();
        //df.sortWithinPartitions("name").show();
        //df.groupBy("sal").agg(df.col("sal")).show();

        //df.sort("sal").agg(df.col("sal")).show();

        //df.withColumn("sal % 1000" ,df.col("sal").divide(1000)).show();

        //df.groupBy("sal").count().show();


        //df.groupBy("sal").count().sort("count").show();

        //df.groupBy("sal").count().withColumnRenamed("count","max count").show();

//        Map<String, String> agg = new HashMap<String, String>();
//        agg.put("sal","sal");
//        agg.put("*", "count");
//        df.agg(agg).show();

        sc.close();
    }

    public static void main4(String[] args) {
        SparkConf conf = new SparkConf().setAppName("myapp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        HiveContext hiveContext = new HiveContext(sc);
        //
        DataFrame df;
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("no", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("sal", DataTypes.IntegerType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("dept", DataTypes.IntegerType, false));
        StructType schema = DataTypes.createStructType(fields);
        df = sqlContext.read().format("com.databricks.spark.csv").schema(schema).option("header", "true")
                .option("inferSchema", "true").load("emp.csv");
        //df.groupBy("dept").max("sal").show();
        df.groupBy("dept", "age").max("sal").show();
        //df.show();
        sc.close();
    }

    public static void main (String[] args) {
        SparkConf conf = new SparkConf().setAppName("myapp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        HiveContext hiveContext = new HiveContext(sc);

        //
        DataFrame empDF;
        List<StructField> empFields = Arrays.asList(
                DataTypes.createStructField("no", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("sal", DataTypes.IntegerType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("dept", DataTypes.IntegerType, false));
        StructType empSchema = DataTypes.createStructType(empFields);
        empDF = sqlContext.read().format("com.databricks.spark.csv").schema(empSchema).option("header", "true")
                .option("inferSchema", "true").load("emp.csv").withColumnRenamed("dept", "deptid");

        DataFrame deptDF;
        List<StructField> deptFields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false));
        StructType deptSchema = DataTypes.createStructType(deptFields);
        deptDF = sqlContext.read().format("com.databricks.spark.csv").schema(deptSchema).option("header", "true")
                .option("inferSchema", "true").load("dept.csv").withColumnRenamed("name", "deptname");
        //df.groupBy("dept").max("sal").show();
        DataFrame joinDF = empDF.join(deptDF, empDF.col("deptid").equalTo(deptDF.col("id"))).select("no", "name", "sal", "age", "deptid", "deptname");
        //joinDF.show();
        //TODO joinDF.groupBy("id").agg(empDF.col("sal"), empDF.col("sal")).show();
        //joinDF.where(" sal > 1500").show();
        //joinDF.groupBy("id", "dept").avg("sal").show();
//        joinDF.select().show();
        Dataset<JoinEmpDept> joinDS = joinDF.select("no", "name", "sal", "age", "deptid", "deptname").as(Encoders.bean(JoinEmpDept.class));
        //joinDS.toDF().show();
//        joinDS.map(new BuildString(), Encoders.STRING()).toDF().show();
        //Dataset<Integer> ages = joinDS.map(set -> set.getAge(), Encoders.INT());
        //ages.show();
        //df.show();
        sc.close();
    }

    public static void main2 (String[] args) {
        SparkConf conf = new SparkConf().setAppName("myapp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        HiveContext hiveContext = new HiveContext(sc);
        //sqlContext.read().text("data.csv").show();
        DataFrame df = null;
        final String readForamt = "parquet";
        if ("csv".equalsIgnoreCase(readForamt)) {
            List<StructField> fields = Arrays.asList(
                    DataTypes.createStructField("no", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("sal", DataTypes.IntegerType, false));
            StructType schema = DataTypes.createStructType(fields);
            df = sqlContext.read().format("com.databricks.spark.csv").schema(schema).option("header", "true")
                    .option("inferSchema", "true").load("data.csv");
        } else if ("parquet".equalsIgnoreCase(readForamt)) {
            df = sqlContext.read().parquet("test.parquet");
        } else if ("json".equalsIgnoreCase(readForamt)) {
            df = sqlContext.read().json("test.json");
        }
        df.printSchema();
        df.show();
        boolean dowrite = false;
        if (dowrite) {
            df.write().format("com.databricks.spark.csv").option("header", "false").save("test.csv");
            df.write().parquet("test.parquet");
            df.write().json("test.json");
            df.write().mode(SaveMode.Overwrite).format("orc").save("test.orc");
            df.write().mode(SaveMode.Overwrite).orc("test.orc");
        }
        sc.close();
    }

    public static void main1 (String[] args) {
        SparkConf conf = new SparkConf();//.setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // DataFrame df = sqlContext.read().json("test.json");
        // df.select("id").show();
        DataFrameReader dfr = sqlContext.read().format("com.databricks.spark.csv").option("header", "true");
        DataFrame df1 = dfr.load("test.csv");
        df1.printSchema();
        df1.select("no", "name").show();
        sqlContext.read();
        sc.close();
    }
}
