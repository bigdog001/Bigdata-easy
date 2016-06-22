/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.fs.Path;
import com.myhadoop.VarKeeper;

/**
 *
 * @author jw362j
 */
public class MyType {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, MyType.class.getSimpleName());
        FileInputFormat.setInputPaths(job, VarKeeper.INPUT_PATH + "/INPUTDATA");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyWriterable.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        job.setCombinerClass(MyReducer.class);//combiner is the local reducer for map method ,this guy will reduce the output data which come from maper ,give lower netword traffic ,improve the performance 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyWriterable.class);
        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, MyWriterable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value != null) {
                String myvalue = value.toString();
                String[] values = myvalue.split(",");
                if (values != null && values.length == 5) {
                    context.write(new Text(values[3]), new MyWriterable(values[0], values[1], values[2], values[3], values[4], Integer.valueOf(values[5])));
                }
            }
        }
    }

    static class MyReducer extends Reducer<Text, MyWriterable, Text, MyWriterable> {

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
