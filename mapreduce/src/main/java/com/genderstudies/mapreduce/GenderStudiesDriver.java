package com.genderstudies.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.genderstudies.mapper.Q1Mapper;


/**
 * Hello world!
 *
 */
public class GenderStudiesDriver
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        if(args.length < 2)
        {
        	System.out.println("Insufficient number of arguments.");
        	System.exit(-1);
        }
        
        Job job = new Job();
        
        job.setJarByClass(GenderStudiesDriver.class);
        
        job.setOutputKeyClass(Text.class);
        
        job.setOutputValueClass(FloatWritable.class);
        
        job.setMapperClass(Q1Mapper.class);
        
        job.setJobName("GenderStudiesQ1");
        
        
        
        
        boolean success = job.waitForCompletion(true);
        
    }
}
