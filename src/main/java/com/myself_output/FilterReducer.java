package com.myself_output;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("reducer...="+key.toString());
        context.write(new Text(key.toString()+"\n"),NullWritable.get());
        System.out.println("reducer...over");
    }
}
