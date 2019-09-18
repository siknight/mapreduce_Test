package com.keyvalue2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class KVTextMapper extends Mapper<Text,Text,Text,IntWritable> {
    static   int i=1;
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key"+i+"="+key);
        System.out.println("value"+i+"="+value);
        i++;
        String[] split = key.toString().split(" ");
        String s=split[0];
        context.write(new Text(s),new IntWritable(1));
    }
}
