package com.Myself_inputformat02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MyDriver.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(MyInputformat02.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);



        FileInputFormat.setInputPaths(job,new Path("D:/testinput/"));
        FileOutputFormat.setOutputPath(job,new Path("D:/test_mapreduce/my26/"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
