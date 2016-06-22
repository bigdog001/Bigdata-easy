package com.bigdata.bigdog.spark;
import com.bigdata.bigdog.spark.bigdog.MyTestLogic;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
public class WordCount implements MyTestLogic,Serializable{
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);
    public static void main(String[] args) {
//        new WordCount().testMe();
    }

    @Override
    public void testMe() {
          // create spark config 
        SparkConf sconf = new SparkConf().setAppName("my spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        
        JavaRDD<String> lines = sc.textFile("C:\\Users\\jw362j\\work_jiunian.wang\\Program_Files\\spark-1.6.0-bin-hadoop2.6\\README.md");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){

            @Override
            public Iterable<String> call(String line) throws Exception {
               String [] words = line.split(" ");
                return Arrays.asList(words);
            }
        });
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {           
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        
       JavaPairRDD<String,Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        
        wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> pairs) throws Exception {
                logger.debug(""+pairs._1+":"+pairs._2);
            }
        });
        
        sc.close();
    }
}
