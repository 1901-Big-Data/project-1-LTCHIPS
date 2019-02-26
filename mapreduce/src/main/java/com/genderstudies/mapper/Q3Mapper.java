package com.genderstudies.mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Q3Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>
{
	private static String[] headers = "Country Name,Country Code,Indicator Name,Indicator Code,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016".split(",+");
	
	private int getColIndex(String col)
	{
		int x;
		
		for (x = 0 ; x < headers.length; x++)
		{
			if (headers[x].equals(col))
				break;
			
		}
		return x;
	}
	
	
	protected void map(LongWritable key, Text row, Context context) throws IOException, InterruptedException
	{	
		
		String[] rowStr = row.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");
		
		if (rowStr[1].equals("\"Country Name\"")) //skip headers
		{
			//headers = rowStr.clone();
			return;
		}
		
		//clean strings
		for (int x = 0; x < rowStr.length; x++)
		{
			rowStr[x] = rowStr[x].substring(1, rowStr[x].length() - 1);	
		}
		
		int index = getColIndex("Indicator Code");
		
		String rowCode = "";
		
		if (rowStr[index].equals("SL.AGR.EMPL.FE.ZS"))
		{
			rowCode = rowStr[1] + " AGR";
		}
		else if (rowStr[index].equals("SL.IND.EMPL.FE.ZS"))
		{
			rowCode = rowStr[1] + " IND";
					
		}
		else if (rowStr[index].equals("SL.SRV.EMPL.FE.ZS"))
		{
			rowCode = rowStr[1] + " SRV";
		}
		
		if (!rowCode.isEmpty())
		{
			for (int x = getColIndex("2000"); x < rowStr.length; x++)
			{
				try
				{
					Float value = Float.parseFloat(rowStr[x]);
					context.write(new Text(rowCode), new FloatWritable(value));
				}
				catch(Exception nfe)
				{
					continue;
				}
			}
		}
		
	}
	
}
