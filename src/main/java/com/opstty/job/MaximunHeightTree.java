package com.opstty.job;

import com.opstty.mapper.MaximunHeightTreeMapper;
import com.opstty.reducer.MaximunHeightTreeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaximunHeightTree
{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: MaximunHeightTree");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "MaximunHeightTree");
        job.setJarByClass(MaximunHeightTree.class);
        job.setMapperClass(MaximunHeightTreeMapper.class);
        job.setCombinerClass(MaximunHeightTreeReducer.class);
        job.setReducerClass(MaximunHeightTreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
