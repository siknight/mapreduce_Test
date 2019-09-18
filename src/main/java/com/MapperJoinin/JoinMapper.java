package com.MapperJoinin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class JoinMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    HashMap<String,String> map=new HashMap<String,String>();

    Text text=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        BufferedReader reader;
        //把这个文件缓冲拿过来
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].getPath()),"UTF-8"));
        String line;
        while(StringUtils.isNotEmpty(line=reader.readLine())){
            String[] split = line.split("\t");
            String pid=split[0];
            String pname=split[1];
            map.put(pid,pname);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        String pid=split[1];
        String pname = map.get(pid);
        text.set(value+"\t"+pname);
        context.write(text,NullWritable.get());

    }
}
