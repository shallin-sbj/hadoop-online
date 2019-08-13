package com.scl.online.hadoop.hdfs.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends
        Reducer<LongWritable, Emplyee, NullWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Emplyee> iter,
                          Context context)
            throws IOException, InterruptedException {

        Emplyee dept = null;
        List<Emplyee> list = new ArrayList<Emplyee>();

        for (Emplyee tmp : iter) {
            if (tmp.getFlag() == 0) {//emp
                Emplyee emplyee = new Emplyee(tmp);
                list.add(emplyee);
            } else {
                dept = new Emplyee(tmp);
            }
        }

        if (dept != null) {
            for (Emplyee emp : list) {
                emp.setDeptName(dept.getDeptName());
                context.write(NullWritable.get(), new Text(emp.toString()));
            }
        }
    }
}