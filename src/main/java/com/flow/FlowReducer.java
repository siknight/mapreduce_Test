package com.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer方法是相同key的values
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


    FlowBean flowBean=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.累加
        long upflowsum=0;
        long downflowsum=0;
        for(FlowBean bean:values){
            upflowsum+=bean.getUpflow();
            downflowsum+=bean.getDownflow();
        }
        //2封装对象
        flowBean.set(upflowsum,downflowsum);
        //3写出去
        context.write(key,flowBean);

    }
}
