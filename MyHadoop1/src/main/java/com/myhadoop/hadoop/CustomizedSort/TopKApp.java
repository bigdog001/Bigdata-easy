/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedSort;

/**
 *
 * @author jw362j
 */
import com.myhadoop.VarKeeper;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 */
public class TopKApp {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(VarKeeper.INPUT_PATH), conf);
        final Path outPath = new Path(VarKeeper.OUTT_PATH);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        final Job job = new Job(conf, TopKApp.class.getSimpleName());
        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

        long max = Long.MIN_VALUE;

        protected void map(LongWritable k1, Text v1, Context context) throws java.io.IOException, InterruptedException {
            final long temp = Long.parseLong(v1.toString());
            if (temp > max) {
                max = temp;
            }
        }

        protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context) throws java.io.IOException, InterruptedException {
            context.write(new LongWritable(max), NullWritable.get());
        }

    }

    static class MyReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

        long max = Long.MIN_VALUE;

        protected void reduce(LongWritable k2, java.lang.Iterable<NullWritable> arg1, org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context arg2) throws java.io.IOException, InterruptedException {
            final long temp = k2.get();
            if (temp > max) {
                max = temp;
            }
        }

        protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context) throws java.io.IOException, InterruptedException {
            context.write(new LongWritable(max), NullWritable.get());
        }

    }
}
