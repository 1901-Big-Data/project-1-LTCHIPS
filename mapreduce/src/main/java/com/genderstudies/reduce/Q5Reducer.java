package com.genderstudies.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{
	protected void reduce(Text key, Iterable<DoubleWritable> dubsVal, Context context) throws IOException, InterruptedException
	{	
		Double max = Double.MIN_VALUE;
		
		List<Double> list = new ArrayList<Double>();
		
		for (DoubleWritable val : dubsVal)
		{
			list.add(val.get());
		}
		
		int fromYearOffset = 0;
		
		for (int x = 0; (x + 1) < list.size(); x++)
		{
			if(list.get(x) == -1.0 || list.get(x + 1) == -1.0)
				continue;
			Double diff = Math.abs(list.get(x) - list.get(x+1));
			if (Double.compare(diff, max) > 0 && list.get(x) > list.get(x + 1))
			{
				max = diff;
				fromYearOffset = x;
			}
		}
		
		StringBuilder fromYearToYearStr = new StringBuilder(key.toString());
		
		//fix for years in key not lining up with CSV's years
		int fromYear = 2004 + fromYearOffset - 1;
		
		int toYear = fromYear + 1;
		
		fromYearToYearStr.append(" (");
		fromYearToYearStr.append(fromYear); 
		fromYearToYearStr.append("-");
		fromYearToYearStr.append(toYear);
		fromYearToYearStr.append(")");
		
		
		if(max != Double.MIN_VALUE)
			context.write(new Text(fromYearToYearStr.toString()), new DoubleWritable(max));
	}
	
}
