package zidingyiu01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ZiMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (String line:split){
            if (line.length()>7){
                context.getCounter("map","大于17").increment(1);
                text.set(value);
                context.write(text,NullWritable.get());
            }else{
                context.getCounter("map","小于17").increment(1);
                System.out.println("长度小于17,,删除掉");
            }
        }
    }
}
