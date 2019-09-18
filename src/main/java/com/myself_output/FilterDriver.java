package com.myself_output;

import javafx.scene.shape.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterDriver {

    /**
     * 1.reducer 阶段 最先进入filterOutputFormat ，然后执行 FilterRecourdWrite构造方法
     * 2.然后进入reducer类的reducer（）方法，然后执行context.write(new Text(key.toString()+"\n"),NullWritable.get())方法
     * 3.在进入FilterRecourdWrite的write方法，方法参数名和reducer()方法参数一样
     *    同时也是一个key执行一次
     *
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FilterDriver.class);

        //设置outputformat的类
        job.setOutputFormatClass(FilterOutputFormat.class);


        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,
                new org.apache.hadoop.fs.Path("d:/log.txt"));
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("d:/test_mapreduce/myout07/"));
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 :1);
    }
}
