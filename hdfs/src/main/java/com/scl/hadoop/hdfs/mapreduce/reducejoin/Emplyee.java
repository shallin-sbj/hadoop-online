package com.scl.hadoop.hdfs.mapreduce.reducejoin;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class Emplyee implements WritableComparable {


    private String empNo = "";
    private String empName = "";
    private String deptNo = "";
    private String deptName = "";
    private int flag = 0;  //区分是员工还是部门

    public Emplyee() {
    }

    public Emplyee(String empNo, String empName, String deptNo, String deptName, int flag) {
        this.empNo = empNo;
        this.empName = empName;
        this.deptNo = deptNo;
        this.deptName = deptName;
        this.flag = flag;
    }

    public Emplyee(Emplyee e) {
        this.empNo = e.empNo;
        this.empName = e.empName;
        this.deptNo = e.deptNo;
        this.deptName = e.deptName;
        this.flag = e.flag;
    }

    @Override
    public int compareTo(Object o) {
        //不做排序
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.empNo);
        dataOutput.writeUTF(this.empName);
        dataOutput.writeUTF(this.deptNo);
        dataOutput.writeUTF(this.deptName);
        dataOutput.writeInt(this.flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.empNo = dataInput.readUTF();
        this.empName = dataInput.readUTF();
        this.deptNo = dataInput.readUTF();
        this.deptName = dataInput.readUTF();
        this.flag = dataInput.readInt();

    }
}
