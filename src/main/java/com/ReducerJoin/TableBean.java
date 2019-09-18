package com.ReducerJoin;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements WritableComparable<TableBean> {
    private String id ;
    private String pid; //key
    private String amount;

    private String pname;
    private  String flag;

    public TableBean() {
    }

    public TableBean(String id, String pid, String amount, String panme, String flag) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pname = panme;
        this.flag = flag;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(amount);
        dataOutput.writeUTF(pname);


        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       id= dataInput.readUTF();
       pid=dataInput.readUTF();
       amount=dataInput.readUTF();
       pname=dataInput.readUTF();
       flag=dataInput.readUTF();

    }

    @Override
    public String toString() {
        return id+"\t"+pname+"\t"+amount;
    }

    @Override
    public int compareTo(TableBean o) {
        return Integer.parseInt(o.getPid()) >Integer.parseInt(this.getPid()) ? -1:1;
    }
}
