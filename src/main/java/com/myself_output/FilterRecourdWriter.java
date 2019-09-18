package com.myself_output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import java.io.IOException;

public class FilterRecourdWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguiguOut =null;
    FSDataOutputStream other =null;
    FileSystem fileSystem=null;

    public FilterRecourdWriter(TaskAttemptContext job) throws IOException {
        //获取客户端
        System.out.println("FilterRecourdWriter构造方法。。。");
        fileSystem = FileSystem.get(new Configuration());
        //输出流
        atguiguOut=fileSystem.create(new Path("D:/test_mapreduce/atguigu.txt"));
        other= fileSystem.create(new Path("D:/test_mapreduce/other.txt"));
        System.out.println("FilterRecourdWriter构造方法结束。。。");
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //获取数据
        String s = text.toString();
        System.out.println("recordwriter...key...="+s);
        //判断 然后将数据写到相应路径
        if (s.contains("atguigu")){
            atguiguOut.write(s.getBytes());
        }else{
            other.write(s.getBytes());  //这里是追加，把每一个key的值追加进去
        }
        System.out.println("recordwriter...key...结束=");

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(other);
        fileSystem.close();
    }
}
