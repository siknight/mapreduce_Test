package com.myself_output02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text,NullWritable> {
    Configuration configuration=new Configuration();
    FSDataOutputStream bing=null;
    FSDataOutputStream other=null;
    FileSystem fs=null;

    public void initial(TaskAttemptContext context) throws IOException {

        configuration=context.getConfiguration();
        fs = FileSystem.get(configuration);
        bing=fs.create(new Path("D:/test_mapreduce/自定义output数据输出/baidu.txt"));
        other=fs.create(new Path("D:/test_mapreduce/自定义output数据输出/other.txt"));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String s = text.toString();
        if (s.contains("baidu")){
            bing.write(s.getBytes());
        }else{
            other.write(s.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(bing);
        IOUtils.closeStream(other);
        fs.close();
    }
}
