/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedIputFormat;

import com.myhadoop.VarKeeper;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author jw362j
 */
public class MyGenericWriteableApp {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(new URI(VarKeeper.OUTT_PATH), conf).delete(new Path(VarKeeper.OUTT_PATH), true);
        Job job = new Job(conf, MyGenericWriteableApp.class.getSimpleName());

        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH);
        
//        MultipleOutputs.addNamedOutput(job, null, args, null, null); //haha .....something new here..        
        MultipleInputs.addInputPath(job, new Path("hdfs://ip:9000/input/inputfilenmae1.txt"), KeyValueTextInputFormat.class,MyMapper4File1.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://ip:9000/input/inputfilenmae2.txt"), TextInputFormat.class,MyMapper4File2.class);
//        job.setMapperClass(MyMapper.class); //d not add this line of code
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyGenericWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));

        job.waitForCompletion(true);
    }

    public static class MyMapper4File1 extends Mapper<Text, Text, Text, MyGenericWritable> {

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, MyGenericWritable>.Context context) throws IOException, InterruptedException {
            String ling = value.toString();
            String s[] = ling.split(",");
            for (String t : s) {
                context.write(new Text(t), new MyGenericWritable(new LongWritable(1)));
            }
        }
    }

    public static class MyMapper4File2 extends Mapper<LongWritable, Text, Text, MyGenericWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MyGenericWritable>.Context context) throws IOException, InterruptedException {
            String ling = value.toString();
            String s[] = ling.split("\t");
            for (String t : s) {
                context.write(new Text(t), new MyGenericWritable(new IntWritable(1)));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, MyGenericWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<MyGenericWritable> values, Context context) throws IOException, InterruptedException {
            int x = 0;
            for (MyGenericWritable w : values) {
                Writable value = w.get();
                if (value instanceof LongWritable) {
                    x += ((LongWritable) value).get();
                }
                if (value instanceof IntWritable) {
                    x += ((IntWritable) value).get();
                }
            }
            context.write(key, new LongWritable(x));
        }

    }

    public static class MyGenericWritable extends GenericWritable {

        public MyGenericWritable(LongWritable lw) {
            super.set(lw);
        }

        public MyGenericWritable(IntWritable iw) {
            super.set(iw);
        }

        public MyGenericWritable() {
        }

        @Override
        protected Class<? extends Writable>[] getTypes() {
            Class[] cls = new Class[]{LongWritable.class, IntWritable.class};
            return cls;
        }

    }
}
