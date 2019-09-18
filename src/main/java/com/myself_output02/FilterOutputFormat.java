package com.myself_output02;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class FilterOutputFormat extends FileOutputFormat<Text,NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        FilterRecordWriter filterRecordWriter = new FilterRecordWriter();
        filterRecordWriter.initial(taskAttemptContext);
        return filterRecordWriter;
    }
}
