package com.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper 方法是对一行数据拆分
 */

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text text=new Text();
    FlowBean flowBean=new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1获取一行
        String v = value.toString();
        //2切割，获取相应的数据
        String[] split = v.split("\t");
        //手机号
        String phone = split[1];
        //上行流量
        String upflow = split[split.length - 3];
        //下行流量
        String downflow = split[split.length - 2];
        //3封装对象
        flowBean.set(Long.parseLong(upflow),Long.parseLong(downflow));
        //4.写出去
        text.set(phone);
        context.write(text,flowBean);


    }
}
