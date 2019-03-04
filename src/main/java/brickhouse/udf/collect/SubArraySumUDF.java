package brickhouse.udf.collect;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class SubArraySumUDF extends UDF {

    public IntWritable evaluate(ArrayList<Integer> list,
                                IntWritable from, IntWritable to) {
        IntWritable result = new IntWritable(0);
        if (list == null || list.size() == 0) {
            return result;
        }

        int m = from.get();
        int n = to.get();

        //m: inclusive, n:exclusive
        List<Integer> subList = list.subList(m, n);

        int sum = 0;
        for (Integer i : subList) {
            sum += i;
        }
        result.set(sum);
        return result;
    }

    public IntWritable evaluate(ArrayList<Integer> list) {
        IntWritable result = new IntWritable(0);
        if (list == null || list.size() == 0) {
            return result;
        }

        int sum = 0;
        for (Integer i : list) {
            sum += i;
        }
        result.set(sum);
        return result;
    }

    public IntWritable evaluate(ArrayList<String> list,String type) {
        IntWritable result = new IntWritable(0);
        if (list == null || list.size() == 0) {
            return result;
        }

        int sum = 0;
        for (String i : list) {
            sum += Integer.valueOf(i);
        }
        result.set(sum);
        return result;
    }
}