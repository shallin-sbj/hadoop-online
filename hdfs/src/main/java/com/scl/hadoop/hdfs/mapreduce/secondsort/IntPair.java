package com.scl.hadoop.hdfs.mapreduce.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int first = 0;
    private int second = 0;

    public void set(int left, int right) {
        first = left;
        second = right;
    }
    public int getFirst() {
        return first;
    }
    public int getSecond() {
        return second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }
    @Override
    public int hashCode() {
        return first+"".hashCode() + second+"".hashCode();
    }
    @Override
    public boolean equals(Object right) {
        if (right instanceof IntPair) {
            IntPair r = (IntPair) right;
            return r.first == first && r.second == second;
        } else {
            return false;
        }
    }
    //这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
    @Override
    public int compareTo(IntPair o) {
        if (first != o.first) {
            return first - o.first;
        } else if (second != o.second) {
            return second - o.second;
        } else {
            return 0;
        }
    }
}
