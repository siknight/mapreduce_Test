package com.ReducerJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> tableBeans01 = new ArrayList<TableBean>(); //存储order
        TableBean pd = new TableBean();


        //同一个key
        for (TableBean tableBean : values){
            System.out.println("tableBean="+tableBean);
              if (tableBean.getFlag().equals("0")){
                  TableBean orderbean = new TableBean();
                  try {
                      BeanUtils.copyProperties(orderbean,tableBean);
                  } catch (IllegalAccessException e) {
                      e.printStackTrace();
                  } catch (InvocationTargetException e) {
                      e.printStackTrace();
                  }
                  tableBeans01.add(orderbean);
              }else if (tableBean.getFlag().equals("1")){
                  try {
                      BeanUtils.copyProperties(pd,tableBean);
                  } catch (IllegalAccessException e) {
                      e.printStackTrace();
                  } catch (InvocationTargetException e) {
                      e.printStackTrace();
                  }
              }
      }
        System.out.println("tableBeans01="+tableBeans01);

        System.out.println("tableBeans02="+pd);

        for (TableBean bean: tableBeans01){
            bean.setPname(pd.getPname());
            context.write(bean,NullWritable.get());
        }


      }


    }

