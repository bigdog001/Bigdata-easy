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
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author jw362j
 *
 * Mysql db table structure: id int(11) , name varchar(32)
 */
public class MyDBInputFormatApp {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test", "root", "123456");
        FileSystem.get(new URI(VarKeeper.OUTT_PATH), conf).delete(new Path(VarKeeper.OUTT_PATH), true);

        Job job = new Job(conf, MyDBInputFormatApp.class.getSimpleName());

        job.setInputFormatClass(DBInputFormat.class);
        DBInputFormat.setInput(job, MyUser.class, "tableName", null, null, "id", "name");

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MyBean.class);

        job.setNumReduceTasks(0);//no reduce ,write the mapper`s output data to HDFS
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(VarKeeper.OUTT_PATH));
        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, MyUser, LongWritable, MyUser> {

        @Override
        protected void map(LongWritable key, MyUser value, org.apache.hadoop.mapreduce.Mapper<LongWritable, MyUser, LongWritable, MyUser>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    private static class MyUser implements Writable, DBWritable {

        private int id;
        private String name;

        public MyUser() {
        }

        public MyUser(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public void write(PreparedStatement ps) throws SQLException {
            ps.setInt(1, id);
            ps.setString(2, name);
        }

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            this.id = rs.getInt(1);
            this.name = rs.getString(2);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(id);
            out.writeBytes(name);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readInt();
            this.name = in.readUTF();
        }

        @Override
        public String toString() {
            return "MyUser{" + "id=" + id + ", name=" + name + '}';
        }
        
        
        
    }
}
