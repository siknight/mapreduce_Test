package com.flow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
//public class FlowBean implements WritableComparable {
    private long upflow;
    private long downflow;
    private long sumflow;

    public FlowBean() {
    }

    public FlowBean(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow=upflow+downflow;
    }

    public void set(long upflow, long downflow){
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow=upflow+downflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }


    @Override
    public String toString() {
        return  "upflow=" + upflow +
                ", downflow=" + downflow +
                ", sumflow=" + sumflow ;
    }

    /**
     * 序列化方法
     * @param dataOutput 框架传来的输出流
     * @throws IOException
     */

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    /**
     * 反序列化方法
     * 注意：反序列化得顺序一定要和序列化的一致
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        upflow=dataInput.readLong();
        downflow=dataInput.readLong();
        sumflow=dataInput.readLong();
    }




//    public int compareTo(Object o) {
//        FlowBean bean=(FlowBean)o;
//        return  this.sumflow>bean.sumflow? -1:1;
//    }
}
