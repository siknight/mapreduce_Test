package com.keyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class KVTextDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获得job
        Configuration conf = new Configuration();
                //设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        //设置jar的路径
        job.setJarByClass(KVTextDriver.class);
        //设置job的mapper和reducer
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextreducer.class);
        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1);

    }
}
