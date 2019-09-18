package com.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取job
        Job job = Job.getInstance(new Configuration());
        //设置job位置
        job.setJarByClass(FlowDriver.class);
        //设置mapper位置
        job.setMapperClass(FlowMapper.class);
        //设置reducer位置
        job.setReducerClass(FlowReducer.class);
        //添加分区
        job.setPartitionerClass(ProvincePartitioner.class);
//        job.setNumReduceTasks(5);
        job.setNumReduceTasks(6);
        //设置mapper输出方式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置reducer输出方式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入参数
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);




    }
}
