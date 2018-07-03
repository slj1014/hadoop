package com.wingconn.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TemperatureDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration ();

        //本地模式运行
        //conf.set ("mapreduce.framework.name","local");

        //设置job的名称
        Job job=Job.getInstance (conf,"temperature");

        //设置主类
        job.setJarByClass (TemperatureDriver.class);


        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(TemperatureReduce.class);
        job.setReducerClass(TemperatureReduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输出输入路径
        FileInputFormat.addInputPath(job, new Path (args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //不显示日志
        //job.submit ();

        //显示日志
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
