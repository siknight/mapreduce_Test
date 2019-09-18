package com.Myself_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private Configuration configuration;
    private FileSplit split;

    private  boolean processed=false;
    private BytesWritable value=new BytesWritable();

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("WholeRecordReader.... initialize...start");
        this.split=(FileSplit) inputSplit;
        System.out.println("WholeRecordReader.... initialize...split="+split.toString());
        configuration=taskAttemptContext.getConfiguration();
        System.out.println("WholeRecordReader.... initialize...over");
    }



    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("WholeRecordReader ....nextKeyValue..start ");
        if (!processed) {
            System.out.println("WholeRecordReader ....nextKeyValue..start .nextKeyValue..processed");
            // 1 定义缓存区，把一个分片的长度的大小直接加到缓冲区中
            byte[] contents = new byte[(int)split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 2 获取文件系统
                Path path = split.getPath();
                //这次文件系统是从分片的path获取的
                fs = path.getFileSystem(configuration);

                // 3 读取数据
                fis = fs.open(path);

                // 4 读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);

                // 5 输出文件内容
                value.set(contents, 0, contents.length);
            } catch (Exception e) {

            }finally {
                IOUtils.closeStream(fis);
            }

            processed = true;

            return true;
        }

        return false;

    }
    //最后返回key，处理的是输入
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        System.out.println("WholeRecordReader ...getCurrentKey....key="+NullWritable.get());
        return NullWritable.get();
    }
  //最后返回value，处理的是输入
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        System.out.println("WholeRecordReader ...getCurrentValue....value="+value);
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        System.out.println("WholeRecordReader ...getProgress....processed="+processed);
        return processed? 1:0;  //每次nextKeyValue执行完后要判断一下如果为true,，每次nextkey执行一个分片，如果为true则nextkey执行完毕，表示这个分片结束了。
    }

    public void close() throws IOException {
        System.out.println("WholeRecordReader ...close");
    }
}
