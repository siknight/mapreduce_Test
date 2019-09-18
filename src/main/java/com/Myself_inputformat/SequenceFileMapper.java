package com.Myself_inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    Text k = new Text();

    @Override
    protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
            throws IOException, InterruptedException {
        System.out.println("SequenceFileMapper.....setup");
        // 1 获取文件切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 2 获取切片名称
        String name = inputSplit.getPath().toString();
        System.out.println("SequenceFileMapper..setup.. splitname="+name);
        // 3 设置key的输出
        k.set(name);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value,
                       Context context)
            throws IOException, InterruptedException {
        System.out.println("map..............start");
        context.write(k, value);
        System.out.println("map..............over");
    }

}
