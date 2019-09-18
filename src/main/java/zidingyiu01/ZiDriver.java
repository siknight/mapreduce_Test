package zidingyiu01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ZiDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ZiDriver.class);
        job.setMapperClass(ZiMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("d:/web.log"));
        FileOutputFormat.setOutputPath(job,new Path("D:/test_mapreduce/ziweblog03"));

        boolean b = job.waitForCompletion(true);
        System.exit(b? 0:1);

    }
}
