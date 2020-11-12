package com.opstty.job;

import com.opstty.mapper.DisplaysSpeciesTreeMapper;
import com.opstty.reducer.DisplaysSpeciesTreeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DisplaysSpeciesTree
{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DisplaysSpeciesTree");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "DisplaysSpeciesTree");
        job.setJarByClass(DisplaysSpeciesTree.class);
        job.setMapperClass(DisplaysSpeciesTreeMapper.class);
        job.setCombinerClass(DisplaysSpeciesTreeReducer.class);
        job.setReducerClass(DisplaysSpeciesTreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}