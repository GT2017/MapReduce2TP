package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighSortingMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
    IntWritable height = new IntWritable();
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        Text values = new Text(value.toString().split(";")[3]);
        if (!value.toString().contains("HAUTEUR")) {
            try{
                height.set((int) Float.parseFloat(value.toString().split(";")[6]));
            }catch (NumberFormatException nbexception){
                height.set(0);
            }
            context.write(height, values);
        }
    }
}
