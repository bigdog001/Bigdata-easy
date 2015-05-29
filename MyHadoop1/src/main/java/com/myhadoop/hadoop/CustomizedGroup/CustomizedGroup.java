/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedGroup;

import com.myhadoop.VarKeeper;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *
 * @author jw362j
 */
public class CustomizedGroup {

    public static void main(String[] args) throws Exception {
        final Configuration configuration = new Configuration();

        final FileSystem fileSystem = FileSystem.get(new URI(VarKeeper.INPUT_PATH), configuration);
        if (fileSystem.exists(new Path(VarKeeper.OUTT_PATH))) {
            fileSystem.delete(new Path(VarKeeper.OUTT_PATH), true);
        }

        final Job job = new Job(configuration, CustomizedGroup.class.getSimpleName());

       
        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH);
        
        job.setInputFormatClass(TextInputFormat.class);
         
        job.setMapperClass(MyMapper.class);
        
        job.setMapOutputKeyClass(NewK2.class);
        job.setMapOutputValueClass(LongWritable.class);

       
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        
        job.setGroupingComparatorClass(MyGroupingComparator.class);
       
        job.setReducerClass(MyReducer.class);
         
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        
        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
        
        job.setOutputFormatClass(TextOutputFormat.class);

        
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable> {

        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NewK2, LongWritable>.Context context) throws java.io.IOException, InterruptedException {
            final String[] splited = value.toString().split("\t");
            final NewK2 k2 = new NewK2(Long.parseLong(splited[0]), Long.parseLong(splited[1]));
            final LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
            context.write(k2, v2);
        }

    }

    static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable> {

        protected void reduce(NewK2 k2, java.lang.Iterable<LongWritable> v2s, org.apache.hadoop.mapreduce.Reducer<NewK2, LongWritable, LongWritable, LongWritable>.Context context) throws java.io.IOException, InterruptedException {
            long min = Long.MAX_VALUE;
            for (LongWritable v2 : v2s) {
                if (v2.get() < min) {
                    min = v2.get();
                }
            }
            context.write(new LongWritable(k2.first), new LongWritable(min));
        }
    }

    /**
     * 
     *
     */
    static class NewK2 implements WritableComparable<NewK2> {

        Long first;
        Long second;

        public NewK2() {
        }

        public NewK2(long first, long second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readLong();
            this.second = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(first);
            out.writeLong(second);
        }

        
        @Override
        public int compareTo(NewK2 o) {
            final long minus = this.first - o.first;
            if (minus != 0) {
                return (int) minus;
            }
            return (int) (this.second - o.second);
        }

        @Override
        public int hashCode() {
            return this.first.hashCode() + this.second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof NewK2)) {
                return false;
            }
            NewK2 oK2 = (NewK2) obj;
            return (this.first == oK2.first) && (this.second == oK2.second);
        }
    }

    
    static class MyGroupingComparator implements RawComparator<NewK2> {

        @Override
        public int compare(NewK2 o1, NewK2 o2) {
            return (int) (o1.first - o2.first);
        }

        
        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                int arg4, int arg5) {
            return WritableComparator.compareBytes(arg0, arg1, 8, arg3, arg4, 8);
        }
    }
}
