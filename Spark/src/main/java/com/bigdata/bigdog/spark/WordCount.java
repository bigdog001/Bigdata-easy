package com.bigdata.bigdog.spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
public class WordCount{
    public static void main(String[] args) {
        
        // create spark config 
        SparkConf sconf = new SparkConf().setAppName("my spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        
    }
}