package com.flow02;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//public class FlowBean implements Writable {
public class FlowBean implements WritableComparable<FlowBean> {
    private long upflow;
    private long downflow;
    private long sumflow;

    public void set(long upflow,long downflow){
        this.upflow=upflow;
        this.downflow=downflow;
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
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       upflow= dataInput.readLong();
       downflow=dataInput.readLong();
       sumflow=dataInput.readLong();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upflow=" + upflow +
                ", downflow=" + downflow +
                ", sumflow=" + sumflow +
                '}';
    }


    @Override
    public int compareTo(FlowBean o) {
        return this.getSumflow()>o.getSumflow()?-1:1;
    }
}
