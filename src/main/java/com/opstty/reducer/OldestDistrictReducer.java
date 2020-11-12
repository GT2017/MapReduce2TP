package com.opstty.reducer;

import com.opstty.OldDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestDistrictReducer extends Reducer<IntWritable, OldDistrictWritable, IntWritable, IntWritable>
{
    private OldDistrictWritable oldest = new OldDistrictWritable(new IntWritable(2020), new IntWritable(0));
    public void reduce(IntWritable key, Iterable<OldDistrictWritable> values, Context context) throws IOException, InterruptedException
    {
        for (OldDistrictWritable vals : values) {
            if(vals.getyear().get()< oldest.getyear().get()){
                oldest.set(new IntWritable(vals.getyear().get()), new IntWritable(vals.getdistrict().get()));
            }
        }
        context.write(oldest.getdistrict(), oldest.getyear());
    }
}
