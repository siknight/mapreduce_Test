package com.keyvalue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {
   Text text=new Text();
   LongWritable w=new LongWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key="+key);
        text.set(key);
        context.write(text,w);
    }
}
