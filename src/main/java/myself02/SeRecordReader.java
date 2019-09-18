package myself02;

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

public class SeRecordReader extends RecordReader<NullWritable,BytesWritable> {
    private FileSplit split;
    private Configuration configuration;
    private boolean progress=false;
    private BytesWritable value=new BytesWritable();
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split=(FileSplit)inputSplit;
        configuration=taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!progress){  //profess为false时执行
            //定义缓冲1区
            byte[] contents = new byte[(int)split.getLength()];
            System.out.println("contents="+contents);
            FileSystem fs= null;

            FSDataInputStream fis=null;

            //try
           fs= FileSystem.get(configuration);
            //获取文件系统
            Path path = split.getPath();
//            path.getFileSystem(configuration);
            //读取数据
            fis=fs.open(path);

            IOUtils.readFully(fis,contents,0,contents.length);

            value.set(contents,0,contents.length);

            IOUtils.closeStream(fis);

            progress=true;
            //文件已读完，可以进入map方法
            return  true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
