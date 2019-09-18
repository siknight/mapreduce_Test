package com.Myself_inputformat02;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable,BytesWritable> {

    Configuration configuration=null;
    FileSplit split=null;


    boolean progressed =false;

    BytesWritable bytesWritable=new BytesWritable();
    //这个方法应该是一个maptask读一次
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("initialize");
        split=(FileSplit)inputSplit;
        configuration=taskAttemptContext.getConfiguration();


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!progressed){
            Path path=null;
            path = split.getPath();

            FileSystem fs=null;
            fs= path.getFileSystem(configuration);


            FSDataInputStream fis=null;
            byte[] bytes = new byte[(int) split.getLength()];
            fis = fs.open(path);


            IOUtils.readFully(fis,bytes,0,bytes.length);

            bytesWritable.set(bytes,0,bytes.length);

            progressed =true;
            IOUtils.closeStream(fis);
            fs.close();
            return  true;
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        System.out.println("关闭流");


    }
}
