package com.genderstudies.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.genderstudies.mapper.Q5MapperMale;
import com.genderstudies.reduce.Q5Reducer;

public class Q5DriverMale extends Configured implements Tool {
	   public static void main( String[] args ) throws Exception
	   {
	   		int exitCode = ToolRunner.run(new Configuration(), new Q5DriverMale(), args);
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
	        
	        job.setJarByClass(Q5Driver.class);
	        
	        job.setOutputKeyClass(Text.class);
	        
	        job.setOutputValueClass(DoubleWritable.class);
	        
	        job.setMapperClass(Q5MapperMale.class);
	        
	        job.setReducerClass(Q5Reducer.class);
	        
	        job.setJobName("GenderStudiesQ5Male");
	        
	        boolean success = job.waitForCompletion(true);
	        
	        return (success == true) ? 1 : 0;
		}
}