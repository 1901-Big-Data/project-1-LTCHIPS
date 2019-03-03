package com.genderstudies.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q3Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
{
	protected void reduce(Text key, Iterable<FloatWritable> fltVal, Context context) throws IOException, InterruptedException
	{	
		ArrayList<Float> vals = new ArrayList<Float>();
		
		for (FloatWritable flt : fltVal)
		{
			vals.add(flt.get());
		}
		
		Float y1 = vals.get(0);
		
		Float y2 = vals.get(1);
		
		Float percentChange = ((y2 - y1)/y1) * 100;
		
		
		if(percentChange != 0.0F)
			context.write(key, new FloatWritable(percentChange));
		
	}
	
}
