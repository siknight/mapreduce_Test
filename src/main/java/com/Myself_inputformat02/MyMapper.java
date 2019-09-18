package com.Myself_inputformat02;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable> {
    Text text=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("setup");
        FileSplit split=(FileSplit)context.getInputSplit();
        String path = split.getPath().toString();
        System.out.println("setup path="+path);
        text.set(path);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(text,value);
    }


}
