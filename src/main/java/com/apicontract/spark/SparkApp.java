package com.apicontract.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by anki on 6/21/2017.
 */
public class SparkApp {

    public static void main(String args[]){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark-app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println("-----------------");
        distData.saveAsTextFile("U:\\pocs\\spark-tutorials\\result");
        sc.stop();
    }
}
