package com.lisi;

import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





import java.io.IOException;

public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job对象
        Job job = Job.getInstance(new Configuration());
        //2指定jar包路径
        job.setJarByClass(WordcountDriver.class);
        //3关联mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5reducer最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.指定job输入的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //7.指定job输出的数据的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        //指定inputformat的类型 如果不设置InputFormat，它默认用的是TextInputFormat.class
       // job.setInputFormatClass(CombineTextInputFormat.class);
        //分片合并
        //设置最大和最小分片大小
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
       // CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

        //注定inputformat类型
//        job.setInputFormatClass(NLineInputFormat.class);
//        NLineInputFormat.setNumLinesPerSplit(job,3);
        //8.提交
        boolean b = job.waitForCompletion(true);
        //退出系统
        System.exit(b ? 0:1);

    }
}
