package myself02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SeDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"D:/testinput02/","D:/test_mapreduce/se01"};
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SeDriver.class);
        job.setMapperClass(SeMapper.class);
        job.setReducerClass(SeReducer.class);
        job.setInputFormatClass(SeInputFormat.class);
        job.setOutputValueClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1);

    }
}
