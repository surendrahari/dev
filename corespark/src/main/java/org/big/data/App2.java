package org.big.data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.*;
import scala.Function1;
import scala.Tuple2;

import java.beans.Encoder;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by hadoop on 4/30/17.
 */
public class App2 {

    public static void main (String[] args) {
        fun4();
    }

    public static void fun4 () {
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("myapp");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        HiveContext hiveContext = new HiveContext(jsc);
        String baseDir = "/Users/hadoop/work/";

        final List<StructField> empFields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.LongType, false, Metadata.empty()),
                DataTypes.createStructField("name", DataTypes.StringType, true, Metadata.empty()),
                DataTypes.createStructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                DataTypes.createStructField("sal", DataTypes.IntegerType, true, Metadata.empty()),
                DataTypes.createStructField("deptid", DataTypes.LongType, true, Metadata.empty()));
        final StructType empSchema = DataTypes.createStructType(empFields);
        DataFrame empDF = sqlContext.read().format("com.databricks.spark.csv").schema(empSchema).option("header", "false").option("inferSchema", "true").load(baseDir + "emp.csv");

        final List<StructField> deptFields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.LongType, false, Metadata.empty()),
                DataTypes.createStructField("name", DataTypes.StringType, true, Metadata.empty()));
        final StructType deptSchema = DataTypes.createStructType(deptFields);
        DataFrame deptDF = sqlContext.read().format("com.databricks.spark.csv").schema(deptSchema).option("header", "false").option("inferSchema", "true").load(baseDir + "dept.csv").withColumnRenamed("id","deptid").withColumnRenamed("name", "deptname");

//        DataFrame joinDF = empDF.join(deptDF, empDF.col("deptid").equalTo(deptDF.col("id")));
//        joinDF.show();
        Column joinCol = empDF.col("deptid").equalTo(deptDF.col("deptid"));
//        Column joinCol = new Column("(deptid = id)");
//        DataFrame joinDF = empDF.join(deptDF, joinCol);
        DataFrame joinDF = empDF.join(deptDF, joinCol).drop("deptid");
        Dataset<EmpDept> ds = joinDF.as(Encoders.bean(EmpDept.class));
        //joinDF.show();
        System.out.println("column : "+ joinCol);
        jsc.close();
    }

    static class MyFilter implements FilterFunction<EmpDept> {
        @Override
        public boolean call (EmpDept empDept) throws Exception {
            if (empDept.getAge() > 20) {
                return true;
            }
            return false;
        }
    }

    static class EmpDept implements Serializable {
        private Long id;
        private String name;
        private Integer age;
        private Integer sal;
        private String deptname;

        public Long getId () {
            return id;
        }

        public void setId (Long id) {
            this.id = id;
        }

        public String getName () {
            return name;
        }

        public void setName (String name) {
            this.name = name;
        }

        public Integer getAge () {
            return age;
        }

        public void setAge (Integer age) {
            this.age = age;
        }

        public Integer getSal () {
            return sal;
        }

        public void setSal (Integer sal) {
            this.sal = sal;
        }

        public String getDeptname () {
            return deptname;
        }

        public void setDeptname (String deptname) {
            this.deptname = deptname;
        }

        @Override
        public String toString () {
            return "EmpDept{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", sal=" + sal +
                    ", deptname='" + deptname + '\'' +
                    '}';
        }
    }


    public static void fun3 () {
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("myapp");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        HiveContext hiveContext = new HiveContext(jsc);
        String baseDir= "/Users/hadoop/work/";
        final List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.LongType, false, Metadata.empty()),
                DataTypes.createStructField("name",DataTypes.StringType, true, Metadata.empty()),
        DataTypes.createStructField("age",DataTypes.IntegerType, true, Metadata.empty()),
        DataTypes.createStructField("sal",DataTypes.IntegerType, true, Metadata.empty()),
        DataTypes.createStructField("deptid",DataTypes.LongType, true, Metadata.empty()));
        final StructType schema = DataTypes.createStructType(fields);
        DataFrame inDF = sqlContext.read().format("com.databricks.spark.csv").schema(schema).option("header","false").option("inferSchema","true").load(baseDir + "test2.csv");
        //DataFrame trDF = inDF.groupBy("deptid").count();
//        DataFrame trDF = inDF.groupBy("deptid").avg("sal");
//        DataFrame trDF = inDF.groupBy("deptid").max("sal").where("max(sal) < 3000");
//        DataFrame trDF = inDF.where("sal < 3000");
//        DataFrame trDF = inDF.where("sal < 3000").groupBy("deptid").count();
        DataFrame trDF2 = inDF.where("sal < 3000");
        //DataFrame trDF = inDF.drop("age").filter("deptid > 1");

//        DataFrame trDF = inDF.unionAll(trDF2);
//        DataFrame trDF = inDF.unionAll(trDF2).dropDuplicates();
//        DataFrame trDF = inDF.unionAll(trDF2).dropDuplicates(new String[]{"age"});
        DataFrame trDF = inDF.except(trDF2);
        //inDF.printSchema();
        //inDF.show();
        //trDF2.show();
        trDF.show();
        jsc.close();
    }

    public static void fun2 () {
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("myapp");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        HiveContext hiveContext = new HiveContext(jsc);
        String baseDir= "/Users/hadoop/work/";
        DataFrame inDF = sqlContext.read().parquet(baseDir + "test.parquet");
        inDF.write().text(baseDir + "test.out");
        //inDF.printSchema();
        //inDF.show();
        jsc.close();
    }


    public static void fun1 () {
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("myapp");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        HiveContext hiveContext = new HiveContext(jsc);
        String baseDir= "/Users/hadoop/work/";
        DataFrame inDF = sqlContext.read().text(baseDir + "test.txt");
        inDF.write().parquet(baseDir + "test.parquet");
        //inDF.write().orc(baseDir+ "test.orc");
        inDF.write().json(baseDir +"test.json");
        jsc.close();
    }


    public static void fun0 (String args[]) {
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("myapp");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        HiveContext hiveContext = new HiveContext(jsc);

        DataFrame empdf = hiveContext.sql("select * from test_keyspace.emp");
        empdf.show();
    }
}
