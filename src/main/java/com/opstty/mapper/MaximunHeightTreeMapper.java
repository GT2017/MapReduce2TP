package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ser.PropertyBuilder;

import java.io.IOException;

public class MaximunHeightTreeMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    IntWritable height = new IntWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("ESPECE")) {
            Text Species = new Text(value.toString().split(";")[3]);
            try{
                height.set((int) Float.parseFloat(value.toString().split(";")[6]));
            }catch (NumberFormatException nbexception){
                height.set(0);
            }
            context.write(Species, height);
        }
    }
}
