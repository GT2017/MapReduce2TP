package com.opstty.mapper;

import com.opstty.OldDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class OldestDistrictMapper extends Mapper<LongWritable, Text, IntWritable, OldDistrictWritable>
{
    private  static IntWritable keyTree = new IntWritable(1);
    private  static OldDistrictWritable OldDistrict = new OldDistrictWritable();
    private  static IntWritable a = null;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if (!value.toString().contains("ARRONDISSEMENT"))
        {
            IntWritable b = new IntWritable(Integer.parseInt(value.toString().split(";")[1]));
            if (!value.toString().split(";")[5].isEmpty())
            {
                a = new IntWritable(Integer.parseInt(value.toString().split(";")[5]));
            }
            if(a != null)
            {
                OldDistrict.set(a,b);
                context.write(keyTree,OldDistrict);
            }
        }
    }
}

