package com.wingconn.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 四个泛型分别代表
 * KeyIn     Reducer的输入数据的Key，这里是每行文字中的“年份”
 * ValueIn   Reducer的输入数据的Value,这里是每行蚊子中的“气温”
 * KeyOut    Reducer的输出数据的Key，这里是不重复的“年份”
 * ValueOut  Reducre的输出数据的Value，这里是这一年中的“最高气温”
 */
public class TemperatureReduce  extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue=Integer.MIN_VALUE;
        StringBuilder sb=new StringBuilder ();
        for(IntWritable value:values){
            maxValue=Math.max (maxValue,value.get ());
            sb.append(value).append(",");
        }
        System.out.print("Before Reduce: " + key + ", " + sb.toString());
        context.write(key, new IntWritable(maxValue));
        System.out.println("======" + "After Reduce: " + key + ", " + maxValue);
    }
}
