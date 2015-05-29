/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedOutputFormat;

import com.myhadoop.VarKeeper;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author jw362j Flush the output result data into difference file according to
 * the output key`s value ,139 guy in 139.txt,138 guys are in 138.txt
 */
public class CustomizedMultipleFilesOutputFormatApplication {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf, CustomizedMultipleFilesOutputFormatApplication.class);
        job.setJarByClass(CustomizedMultipleFilesOutputFormatApplication.class);
        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH + "/INPUTDATA");
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormat(CustomizedMultipleFilesOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
        JobClient.runJob(job);
    }

    static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable k1, Text v1, OutputCollector<Text, LongWritable> oc, Reporter rprtr) throws IOException {
            if (v1 != null) {
                String myvalue = v1.toString();
                String[] values = myvalue.split(",");
                if (values != null && values.length == 5) {
                    oc.collect(new Text(values[3]), new LongWritable(1L));
                }
            }
        }
    }

    static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text k2, Iterator<LongWritable> itrtr, OutputCollector<Text, LongWritable> oc, Reporter rprtr) throws IOException {
            Long counter = 0L;
            while (itrtr.hasNext()) {
                LongWritable object = itrtr.next();
                counter += object.get();
            }
            oc.collect(k2, new LongWritable(counter));
        }
    }

    static class CustomizedMultipleFilesOutputFormat extends MultipleOutputFormat<Text, LongWritable> {

        @Override
        protected org.apache.hadoop.mapred.RecordWriter<Text, LongWritable> getBaseRecordWriter(FileSystem fs, JobConf jc, String string, Progressable p) throws IOException {
            return new TextOutputFormat<Text, LongWritable>().getRecordWriter(fs, jc, string, p);
        }

        @Override
        protected String generateFileNameForKeyValue(Text key, LongWritable value, String name) {
            String fileName = "";
            if (key.toString().contains("139")) {
                fileName = "139.txt";
            }
            if (key.toString().contains("131")) {
                fileName = "131.txt";
            } else {
                fileName = "186.txt";
            }
            return fileName;
        }

    }

}
