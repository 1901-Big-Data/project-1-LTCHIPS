package com.genderstudies.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.genderstudies.mapper.Q4Mapper;
import com.genderstudies.reduce.Q3Reducer;

public class Q4Driver extends Configured implements Tool {
	   public static void main( String[] args ) throws Exception
	    {
	    	int exitCode = ToolRunner.run(new Configuration(), new Q4Driver(), args);
			System.exit(exitCode);        
	    }

		@Override
		public int run(String[] arg0) throws Exception {
			if(arg0.length < 2)
	        {
	        	System.out.println("Insufficient number of arguments.");
	        	System.exit(-1);
	        }
	        
	        Job job = new Job(getConf());
	        
			FileInputFormat.setInputPaths(job, new Path(arg0[0]));
			FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
	        
	        job.setJarByClass(Q4Driver.class);
	        
	        job.setOutputKeyClass(Text.class);
	        
	        job.setOutputValueClass(Text.class);
	        
	        job.setMapperClass(Q4Mapper.class);
	        
	        job.setReducerClass(Q3Reducer.class);
	        
	        job.setJobName("GenderStudiesQ4");
	        
	        boolean success = job.waitForCompletion(true);
	        
	        return (success == true) ? 1 : 0;
		}
}
