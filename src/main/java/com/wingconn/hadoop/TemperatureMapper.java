package com.wingconn.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 四个泛型类型分别代表
 * KeyIn    Mapper的输入数据的Key，这里是每行蚊子的其实位置（0,1,2）
 * ValueIn  Mapper的输入数据的Value，这里是每行蚊子
 * KeyOut   Mapper的输出数据的Key，这里是每行文字中的“年份”
 * ValueOut Mapper的输出数据的Value,这里是每行文字中的“气温”
 */
public class TemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable>  {
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

