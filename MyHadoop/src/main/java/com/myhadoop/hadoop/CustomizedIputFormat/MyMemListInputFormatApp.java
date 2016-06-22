/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hadoop.CustomizedIputFormat;

import com.myhadoop.VarKeeper;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author jw362j
 */
public class MyMemListInputFormatApp {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, MyMemListInputFormatApp.class.getSimpleName());
        FileInputFormat.setInputPaths(job, new Path(VarKeeper.INPUT_PATH));
        job.setInputFormatClass(MyInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MyBean.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, MyBean, LongWritable, MyBean> {

        @Override
        protected void map(LongWritable key, MyBean value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }

    }

    public static class MyReducer extends Reducer<LongWritable, MyBean, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<MyBean> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (MyBean mb : values) {
                total += mb.getScore();
            }
            context.write(key, new Text(total + ""));
        }
    }

    public static class MyInputFormat extends InputFormat {

        @Override
        public List<InputSplit> getSplits(JobContext jc) throws IOException, InterruptedException {
            List<InputSplit> spls = new ArrayList<InputSplit>();
            spls.add(new mySplit());
            spls.add(new mySplit());
            spls.add(new mySplit());
            spls.add(new mySplit());
            spls.add(new mySplit());
            return spls;
        }

        @Override
        public RecordReader<LongWritable, MyBean> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            return new MyReader();
        }
    }

    public static class mySplit extends InputSplit implements Writable {

        final ArrayWritable aw = new ArrayWritable(MyBean.class);

        final int split_size = 10;

        public mySplit() {
            MyBean[] mb = new MyBean[split_size];
            for (int i = 0; i < split_size; i++) {
                mb[i] = new MyBean(i, "wang" + i, i * 10 - i);
            }
            aw.set(mb);
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return split_size;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[]{};
        }

        public ArrayWritable getSplit() {
            return aw;
        }

        @Override
        public void write(DataOutput d) throws IOException {
            aw.write(d);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            aw.readFields(in);
        }

    }

    private static class MyReader extends RecordReader<LongWritable, MyBean> {

        private Writable[] aw;
        private LongWritable c_key;
        private MyBean mb;
        private int total_record = 0;
        private int pos = 0;

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            mySplit ms = (mySplit) is;
            aw = ms.getSplit().get();
            total_record = aw.length;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean result = false;
            if (pos < total_record) {
                mb = (MyBean) aw[pos];
                c_key = new LongWritable(mb.getId());
                pos++;
                result = true;
            } else {
                mb = null;
                result = false;
            }
            return result;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return c_key;
        }

        @Override
        public MyBean getCurrentValue() throws IOException, InterruptedException {
            return mb;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return ((float) pos) / ((float) total_record);
        }

        @Override
        public void close() throws IOException {

        }
    }

}

class MyBean implements Writable {

    private int id;
    private String username;
    private int score;

    public MyBean() {
    }

    public MyBean(int id, String username, int score) {
        this.id = id;
        this.username = username;
        this.score = score;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeBytes(username);
        out.writeInt(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.username = in.readUTF();
        this.score = in.readInt();
    }

    @Override
    public String toString() {
        return "MyBean{" + "id=" + id + ", username=" + username + ", score=" + score + '}';
    }

}
