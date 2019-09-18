package com.Myself_inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeFileInputformat extends FileInputFormat<NullWritable, BytesWritable> {
   //有输入文件计算出分片（inputsplit），解决数据分割成片问题
//    @Override
//    protected boolean isSplitable(JobContext context, Path filename) {
//        System.out.println("WholeFileInputformat。。。isSplitable");
//        return  false;
//    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("WholeFileInputformat。。。RecordReader");

        WholeRecordReader recordReader=new WholeRecordReader();
        //  recordReader.initialize(inputSplit,taskAttemptContext);
        return recordReader;
    }
}
