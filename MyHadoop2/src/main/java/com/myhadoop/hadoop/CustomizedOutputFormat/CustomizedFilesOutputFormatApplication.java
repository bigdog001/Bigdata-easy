/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedOutputFormat;

import com.myhadoop.VarKeeper;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *
 * @author jw362j
 */
public class CustomizedFilesOutputFormatApplication {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, CustomizedFilesOutputFormatApplication.class.getSimpleName());
        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH + "/INPUTDATA");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(CustomizedTextFilesOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value != null) {
                String myvalue = value.toString();
                String[] values = myvalue.split(",");
                if (values != null && values.length == 5) {
                    context.write(new Text(values[3]), new LongWritable(1L));
                }
            }
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            if (values != null) {
                for (LongWritable value : values) {
                    counter += value.get();
                }
                context.write(new Text(key.toString()), new LongWritable(counter));
            }
        }
    }

    static class CustomizedTextFilesOutputFormat extends OutputFormat<Text, LongWritable> {

        private static final String OUT_DES = "hdfs://somewhere:9000/OUT/";
        private FileSystem fs = null;
        private FSDataOutputStream fsos = null;

        @Override
        public org.apache.hadoop.mapreduce.RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
            try {
                fs = FileSystem.get(new URI(OUT_DES), tac.getConfiguration());
                fsos = fs.create(new Path(OUT_DES + "/asd.txt"));
            } catch (Exception e) {
                e.printStackTrace();
            }

            return new RecordWriter<Text, LongWritable>() {

                @Override
                public void write(Text k, LongWritable v) throws IOException, InterruptedException {
                    if (fsos != null) {
                        fsos.writeBytes(k.toString());
                        fsos.writeBytes("\t");
                        fsos.writeBytes(v.get() + "");
                    }
                }

                @Override
                public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
                    if (fsos != null) {
                        fsos.close();
                    }
                }

            };
        }

        @Override
        public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {

        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException, InterruptedException {
            return new FileOutputCommitter(new Path("myoutDestination"), tac);
        }
    }
}
