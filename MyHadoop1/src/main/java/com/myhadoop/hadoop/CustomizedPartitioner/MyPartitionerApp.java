/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedPartitioner;

import com.myhadoop.hadoop.CustomizedType.MyType;
import com.myhadoop.VarKeeper;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
public class MyPartitionerApp {

    public static void main(String[] args) throws Exception {
        final Job job = new Job(new Configuration(), MyPartitionerApp.class.getSimpleName());
        job.setJarByClass(MyPartitionerApp.class);
         
        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH);
        
        job.setInputFormatClass(TextInputFormat.class);

        
        job.setMapperClass(MyMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyWriterable.class);

         
        job.setPartitionerClass(MyPartitioner.class);//give the partition calss type here
        job.setNumReduceTasks(2); // the function which named MyPartitioner located in MyPartitioner return only 2 kinds of values ,0 and 1,so here should be 2

         
        job.setReducerClass(MyReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyWriterable.class);

        
        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
         
        job.setOutputFormatClass(TextOutputFormat.class);

        
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, MyWriterable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            if (value != null) {
                String myvalue = value.toString();
                String[] values = myvalue.split(",");
                if (values != null && values.length == 5) {
                    context.write(new Text(values[3]), new MyWriterable(values[0], values[1], values[2], values[3], values[4], Integer.valueOf(values[5])));
                }
            }
        }
    }

    static class MyReducer extends Reducer< Text, MyWriterable, Text, MyWriterable> {

        @Override
        protected void reduce(Text key, Iterable<MyWriterable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            if (values != null) {
                for (MyWriterable value : values) {
                    counter += value.size;
                }
                context.write(new Text(key.toString()), new MyWriterable("", "", "", "", "", counter));
            }
        }
    }

    static class MyPartitioner extends HashPartitioner<Text, MyWriterable> {

        @Override
        public int getPartition(Text key, MyWriterable value, int i) {
            return (key.toString().contains("139")) ? 0 : 1; //Put 139 guy in one partition by return the value 0, put some guys else in the other partition by return the value 1,so 2 patitions are there totally
        }

    }

    static class MyWriterable implements Writable {

        private String userid = "";
        private String username = "";
        private String userAddress = "";
        private String usercellphone = "";
        private String userpassword = "";
        private int size = 0;

        public MyWriterable() {
        }

        public MyWriterable(String userid, String username, String userAddress, String usercellphone, String userpassword, int size) {
            this.userid = userid;
            this.username = username;
            this.userAddress = userAddress;
            this.usercellphone = usercellphone;
            this.userpassword = userpassword;
            this.size = size;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(userid);
            out.writeUTF(username);
            out.writeUTF(userAddress);
            out.writeUTF(usercellphone);
            out.writeUTF(userpassword);
            out.writeInt(size);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            userid = in.readUTF();
            username = in.readUTF();
            userAddress = in.readUTF();
            usercellphone = in.readUTF();
            userpassword = in.readUTF();
            size = in.readInt();
        }

        @Override
        public String toString() {
            return "MyWriterable{" + "userid=" + userid + ", username=" + username + ", userAddress=" + userAddress + ", usercellphone=" + usercellphone + ", userpassword=" + userpassword + ", size=" + size + '}';
        }

    }

}
