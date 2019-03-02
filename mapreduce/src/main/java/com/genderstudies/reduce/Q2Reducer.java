package com.genderstudies.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
{
	protected void reduce(Text key, Iterable<FloatWritable> fltVal, Context context) throws IOException, InterruptedException
	{
		int numOfEntries = 0;
		float sum = 0;
		
		List<Float> list = new ArrayList<Float>();
		
		for (FloatWritable val : fltVal)
		{
			list.add(val.get());
		}
		
		for (int x = 0; (x + 1) < list.size(); x++)
		{
			sum+=Math.abs((list.get(x) - list.get(x+1)));
			numOfEntries++;
		}
		
		float average = sum/numOfEntries;
		
		context.write(key, new FloatWritable(average));
		
	}

}
