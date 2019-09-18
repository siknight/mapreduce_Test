package com.myself_output;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        text.set(s);
        context.write(text,NullWritable.get());
    }
}
