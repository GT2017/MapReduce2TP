package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import javax.xml.crypto.Data;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OldDistrictWritable implements WritableComparable {
    public IntWritable year;
    public IntWritable district;

    public OldDistrictWritable(){
        this.year = new IntWritable();
        this.district = new IntWritable();
    }

    public OldDistrictWritable(IntWritable myyear,IntWritable mydistrict){
        this.year = myyear;
        this.district = mydistrict;
    }

    public IntWritable getyear(){
        return year;
    }

    public IntWritable getdistrict(){
        return district;
    }

    public void set(IntWritable year, IntWritable district)
    {
        this.year = year;
        this.district = district;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        district.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        district.readFields(dataInput);
        year.readFields(dataInput);
    }
}
