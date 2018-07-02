package com.wingconn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by albert.shen on 2018/7/2.
 */
public class Temperature{
    /**
     * 四个泛型类型分别代表
     * KeyIn    Mapper的输入数据的Key，这里是每行蚊子的其实位置（0,1,2）
     * ValueIn  Mapper的输入数据的Value，这里是每行蚊子
     * KeyOut   Mapper的输出数据的Key，这里是每行文字中的“年份”
     * ValueOut Mapper的输出数据的Value,这里是每行文字中的“气温”
     */
    static class TempMappper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println ("Before Mapper:"+key+","+value);
            String line=value.toString ();
            String year=line.substring (0,4);
            int temperature = Integer.parseInt(line.substring(8));
            context.write (new Text (year),new IntWritable (temperature));
            System.out.println ("========After Mapper:"+new Text (year)+","+","+new IntWritable (temperature) );
        }
    }

    /**
     * 四个泛型分别代表
     * KeyIn     Reducer的输入数据的Key，这里是每行文字中的“年份”
     * ValueIn   Reducer的输入数据的Value,这里是每行蚊子中的“气温”
     * KeyOut    Reducer的输出数据的Key，这里是不重复的“年份”
     * ValueOut  Reducre的输出数据的Value，这里是这一年中的“最高气温”
     */
    static class TempReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
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

    public static  void main(String[] args){
        //输入路径
        String dst = "hdfs://node-1:9000/input.txt";
        //输出路径，必须是不存在的，空文件夹也不行。
        String dstOut = "hdfs://node-1:9000/output.txt";

        Configuration hadoopConfig = new Configuration();
    }
}
