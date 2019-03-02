package com.genderstudies.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StdDeviationReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
{
	protected void reduce(Text key, Iterable<FloatWritable> fltVal, Context context) throws IOException, InterruptedException
	{	
		ArrayList<Float> vals = new ArrayList<Float>();
		
		float average = 0;
		
		for (FloatWritable flt : fltVal)
		{
			average += flt.get();
			vals.add(flt.get());
		}
		average/=vals.size();
		
		float devNumerator = 0;
		
		for (Float value : vals)
		{
			devNumerator+=(float) Math.pow(value-average, 2.0);
		}
		
		devNumerator/=(vals.size() - 1);
		
		Float result = (float) Math.sqrt((double)devNumerator);
		
		context.write(key, new FloatWritable(result));
		
	}
	
}
