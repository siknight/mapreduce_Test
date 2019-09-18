package com.keyvalue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVTextreducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    Text text=new Text();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum =0L;
        for (LongWritable w:values){
            sum += w.get();
        }
        text.set(key);
        //输出
        context.write(text,new LongWritable(sum));
    }
}
