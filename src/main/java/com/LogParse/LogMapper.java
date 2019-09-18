package com.LogParse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(" ");
        if (split.length>11){

            text.set(s);
            context.getCounter("map","大于11").increment(1);
            context.write(text,NullWritable.get());
        }else{
            context.getCounter("map","小于11").increment(1);
            System.out.println("什么也不做");
        }
    }
}
