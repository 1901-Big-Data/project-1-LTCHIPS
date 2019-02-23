package com.genderstudies.reduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
{
	protected void reduce(Text key, Iterable<FloatWritable> fltVal, Context context) throws IOException, InterruptedException
	{
		int numOfEntries = 1;
		float sum = 0;
		for(FloatWritable val : fltVal)
		{
			sum+=val.get();
		}
		
		float average = sum/numOfEntries;
		
		if (average < 30.0F)
		{
			context.write(key, new FloatWritable(average));
		}
		
	}
}
