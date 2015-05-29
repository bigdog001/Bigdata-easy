/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CmdParameter;

import com.myhadoop.hadoop.CustomizedCounter.MyCounter;
import com.myhadoop.VarKeeper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author jw362j
 */
public class CmdParemeterApp extends Configured implements Tool {

    private static String BadWord = "byebye";
    private static Counter mycounter;

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, MyCounter.class.getSimpleName());
        FileInputFormat.setInputPaths(job, strings[0] );
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CmdParemeterApp(), args);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            mycounter = context.getCounter("My customized counter name", BadWord);
            if (value.toString().contains(BadWord)) {
                mycounter.increment(1L);//counter change the value                 
            }
        }

    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //do nothing...
        }

    }
}
