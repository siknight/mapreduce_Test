package myself02;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SeMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable> {
    Text k=new Text();

    //每个分片设置一个key
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit inputsplit=(FileSplit)context.getInputSplit();
        System.out.println("inputSlit_="+inputsplit);
        String name=inputsplit.getPath().toString();
        System.out.println("name_="+name);
        k.set(name);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(k,value);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
