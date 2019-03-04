package com.genderstudies.reduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q3Reducer extends Reducer<Text, Text, Text, FloatWritable> 
{
	protected void reduce(Text key, Iterable<Text> fltVal, Context context) throws IOException, InterruptedException
	{	
		Float y1 = 0.0F;
		
		Float y2 = 0.0F;
		
		for (Text flt : fltVal)
		{
			String fltStr = flt.toString();
			if (fltStr.equals("y1"))
			{
				y1 = Float.parseFloat(flt.toString().substring(4));
			}
			else if (fltStr.equals("y2"))
			{
				y2 = Float.parseFloat(flt.toString().substring(4));
			}
		}
		
		Float percentChange = ((y2 - y1)/y1) * 100;
		
		
		if(percentChange != 0.0F)
			context.write(key, new FloatWritable(percentChange));
		
	}
	
}
