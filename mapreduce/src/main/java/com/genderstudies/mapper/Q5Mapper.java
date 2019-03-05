package com.genderstudies.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q5Mapper  extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	private static String[] headers = 
		{ "Country Name",
		"Country Code",
		"Indicator Name",
		"Indicator Code",
		"1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"
		};	
	
	
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
		//the regex splits on commas NOT enclosed in double quotes
		String[] rowStr = row.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");
		
		
		//skip headers
		if (rowStr[1].equals("\"Country Name\""))
		{
			return;
		}
		
		int index = getColIndex("Indicator Code");
		
		if (rowStr[index].equals("\"SE.TER.HIAT.BA.FE.ZS\""))
		{	
			String countryName = rowStr[0].substring(1, rowStr[0].length() - 1);
			
			//48 is the year 2004 column
			for (int x = 48; x < rowStr.length; x++)
			{	
				try
				{
					String thingToParse = rowStr[x].substring(1, rowStr[x].length() - 1);
					Double value = Double.parseDouble(thingToParse);
					context.write(new Text(countryName), new DoubleWritable(value));
				}
				catch(NumberFormatException nfe)
				{
					context.write(new Text(countryName), new DoubleWritable(-1.0));
					continue;
				}
			}
		}
		
	}
	
}
