package com.flow02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<LongWritable,FlowBean,LongWritable,FlowBean> {
   FlowBean flowBean= new FlowBean();
    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upflow=0;
        long downflow=0;
        for (FlowBean bean: values){
            upflow+=bean.getUpflow();
            downflow+=bean.getDownflow();
        }
        flowBean.set(upflow,downflow);


        context.write(key,flowBean);

    }
}
