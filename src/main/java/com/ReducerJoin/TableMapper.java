package com.ReducerJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {
    String name=null;
    Text text=new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (name.startsWith("order")){
            TableBean tableBean = new TableBean();
            String[] split = value.toString().split("\t");

            tableBean.setId(split[0]);
            tableBean.setPid(split[1]);
            tableBean.setAmount(split[2]);
            tableBean.setFlag("0");
            tableBean.setPname("");
            text.set(split[1]);
            System.out.println("pid01="+split[1]);
            System.out.println(" map tablebean01="+tableBean.toString());
            context.write(text,tableBean);

        }else{
            String[] split = value.toString().split("\t");
            TableBean tableBean = new TableBean();
            tableBean.setPid(split[0]);
            System.out.println("pname="+split[1]);

            tableBean.setPname(split[1]);
            tableBean.setFlag("1");
            tableBean.setAmount("");
            tableBean.setId("");
            text.set(split[0]);
            System.out.println("pid02="+split[0]);
            System.out.println("map tablebean02="+tableBean.toString());
            context.write(text,tableBean);

        }
    }
}
