package com.flow02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,LongWritable,FlowBean> {
    LongWritable w=new LongWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split2=(FileSplit) context.getInputSplit();
        System.out.println("切片大小="+split2.getPath().getName());

        String[] split = value.toString().split("\t");
        Long phone=Long.parseLong(split[1]);
        long downflow = Long.parseLong(split[split.length - 2]);
        long upflow = Long.parseLong(split[split.length - 3]);
        FlowBean flowBean = new FlowBean();
        flowBean.set(upflow,downflow);
        w.set(phone);
        context.write(w,flowBean);

    }
}
