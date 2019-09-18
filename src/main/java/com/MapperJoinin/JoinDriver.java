package com.MapperJoinin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class JoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(JoinDriver.class);
        job.addCacheFile(new URI("file:///D:/tableinput/pd.txt"));

        job.setMapperClass(JoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job,new Path("D:/tableinput/"));
        FileOutputFormat.setOutputPath(job,new Path("D:/test_mapreduce/mapjoin07/"));

        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1 );


    }
}
