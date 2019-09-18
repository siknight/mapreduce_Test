package com.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区
 */

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    /**
     * 
     * @param key  这里的key指的是mapper的context写出的类
     * @param flowBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        String preNum  = key.toString().substring(0, 3); //截取手机号前3位
        int partition=4; //4个分区，默认
        if ("136".equals(preNum)){
            partition=0;  //第一个分区
        }else if ("137".equals(preNum)) {
            partition = 1; //第二个分区
        }else if ("138".equals(preNum)){
            partition=2;  //第三个分区
        }else if ("139".equals(preNum)) {
            partition = 3;  //第四个分区
        }
        return partition;   //返回分区数
    }
}
