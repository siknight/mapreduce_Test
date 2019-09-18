package com.flow02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartition extends Partitioner<LongWritable,FlowBean> {


    @Override
    public int getPartition(LongWritable text, FlowBean flowBean, int i) {
        String str = text.toString().substring(0, 3);
        int partition=4;
        if ("136".equals(str)){
            partition=0;
        }else if ("137".equals(str)){
            partition=1;
        }else if("138".equals(str)){
            partition=2;
        }else if("139".equals(str)){
            partition=3;
        }


        return partition;
    }
}
