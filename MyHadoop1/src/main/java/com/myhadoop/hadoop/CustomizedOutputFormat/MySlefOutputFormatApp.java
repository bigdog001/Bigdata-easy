package com.myhadoop.hadoop.CustomizedOutputFormat;
import java.io.IOException;  
import java.net.URI;  
import java.net.URISyntaxException;    
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.JobContext;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.OutputCommitter;  
import org.apache.hadoop.mapreduce.OutputFormat;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;    
/**
 * 自定义OutputFormat， 指定输出文件名，并对输出的key-value在不同key下分别输出到不同目标文件 
 * 执行结果： 
 * [root@master hadoop]# hadoop fs -lsr / 
Warning: $HADOOP_HOME is deprecated. 
 
-rw-r--r--   3 zm supergroup         19 2014-12-02 04:17 /hello 
-rw-r--r--   3 zm supergroup         19 2014-12-02 04:16 /hello2 
drwxr-xr-x   - zm supergroup          0 2014-12-04 00:17 /out 
-rw-r--r--   3 zm supergroup          8 2014-12-04 00:17 /out/abc 
-rw-r--r--   3 zm supergroup         11 2014-12-04 00:17 /out/other 
 
 
[root@master hadoop]# hadoop fs -text /out/other 
Warning: $HADOOP_HOME is deprecated. 
 
me      1 
you     1 
[root@master hadoop]# hadoop fs -text /out/abc 
Warning: $HADOOP_HOME is deprecated. 
 
hello   2 
 * @author zm 
 * 
 */  
//  http://chengjianxiaoxue.iteye.com/blog/2163284
public class MySlefOutputFormatApp {  
    private static final String INPUT_PATH = "hdfs://master:9000/hello";  
    private static final String OUT_PATH = "hdfs://master:9000/out";  
    private static final String OUT_FIE_NAME = "/abc";  
    private static final String OUT_FIE_NAME1 = "/other";  
  
    public static void main(String[] args) throws Exception{  
        // 定义conf  
        Configuration conf = new Configuration();  
        final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);  
        if(filesystem.exists(new Path(OUT_PATH))){  
            filesystem.delete(new Path(OUT_PATH), true);  
        }  
        // 定义job  
        final Job job = new Job(conf , MySlefOutputFormatApp.class.getSimpleName());  
        job.setJarByClass(MySlefOutputFormatApp.class);  
          
        // 定义InputFormat  
        FileInputFormat.setInputPaths(job, INPUT_PATH);  
        job.setInputFormatClass(TextInputFormat.class);  
        // 定义map  
        job.setMapperClass(MyMapper.class);  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(LongWritable.class);  
        // 定义reduce  
        job.setReducerClass(MyReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(LongWritable.class);  
        // 定义OutputFormat 自定义的MySelfTextOutputFormat中指定了输出文件名称  
        job.setOutputFormatClass(MySelfTextOutputFormat.class);  
          
        job.waitForCompletion(true);  
    }  
      
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{  
        //  
        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {  
            //  
            final String line = value.toString();  
            final String[] splited = line.split("\t");  
              
            //  
            for (String word : splited) {  
                context.write(new Text(word), new LongWritable(1));  
            }  
        };  
    }  
      
    //map产生的<k,v>分发到reduce的过程称作shuffle  
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{  
        protected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {  
            //  
            long count = 0L;  
            for (LongWritable times : values) {  
                count += times.get();  
            }  
            context.write(key, new LongWritable(count));  
        };  
    }  
      
      
    // 自定义OutputFormat，定义好输出数据的FSDataOutputStream位置  
    public static class MySelfTextOutputFormat extends OutputFormat<Text, LongWritable>{  
        FSDataOutputStream outputStream =  null;// 自定义OutputFormat中，通过FSDataOutputStream将数据写出到hdfs中  
        FSDataOutputStream outputStream1 =  null;  
        @Override  
        public RecordWriter<Text, LongWritable> getRecordWriter(  
                TaskAttemptContext context) throws IOException,  
                InterruptedException {  
            try {  
                final FileSystem fileSystem = FileSystem.get(new URI(MySlefOutputFormatApp.OUT_PATH), context.getConfiguration());  
                //指定的是输出文件路径  
                final Path opath = new Path(MySlefOutputFormatApp.OUT_PATH+OUT_FIE_NAME);  
                final Path opath1 = new Path(MySlefOutputFormatApp.OUT_PATH+OUT_FIE_NAME1);  
                if(fileSystem.exists(opath)){  
                    fileSystem.delete(opath,true);  
                }  
                if(fileSystem.exists(opath1)){  
                    fileSystem.delete(opath1,true);  
                }  
                  
                this.outputStream = fileSystem.create(opath, false);  
                this.outputStream1 = fileSystem.create(opath1, false);  
            } catch (URISyntaxException e) {  
                e.printStackTrace();  
            }  
            // 自定义 RecordWriter  
            return new MySlefRecordWriter(outputStream,outputStream1);  
        }  
  
        @Override  
        public void checkOutputSpecs(JobContext context) throws IOException,  
                InterruptedException {  
              
        }  
  
        @Override  
        public OutputCommitter getOutputCommitter(TaskAttemptContext context)  
                throws IOException, InterruptedException {  
            return new FileOutputCommitter(new Path(MySlefOutputFormatApp.OUT_PATH), context);  
        }  
  
    }  
      
    // 自定义 RecordWriter  
    public static class MySlefRecordWriter extends RecordWriter<Text, LongWritable>{  
        FSDataOutputStream outputStream = null;  
        FSDataOutputStream outputStream1 = null;  
  
        public MySlefRecordWriter(FSDataOutputStream outputStream,FSDataOutputStream outputStream1) {  
            this.outputStream = outputStream;  
            this.outputStream1 = outputStream1;  
        }  
  
        @Override  
        public void write(Text key, LongWritable value) throws IOException,  
                InterruptedException {  
              
            if(key.toString().equals("hello")){  
                outToHdfs(outputStream,key, value);  
            }else{  
                outToHdfs(outputStream1,key, value);  
            }  
              
        }  
  
        private void outToHdfs(FSDataOutputStream outputStream,Text key, LongWritable value) throws IOException {  
            outputStream.writeBytes(key.toString());  
            outputStream.writeBytes("\t");  
            //this.outputStream.writeLong(value.get()); 这种方式输出的话，在hdfs中是不会正常显示的 需要用如下方式才可  
            outputStream.writeBytes(value.toString());  
            outputStream.writeBytes("\n");  
        }  
  
        @Override  
        public void close(TaskAttemptContext context) throws IOException,  
                InterruptedException {  
            this.outputStream.close();  
        }  
          
    }  
}  
