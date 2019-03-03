package com.genderstudies.mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q3Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>
{
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
		
		if (rowStr[1].equals("\"Country Name\"")) //skip headers
		{
			return;
		}
		
		int index = getColIndex("Indicator Code");
		
		String rowCode = "";
		
		if (rowStr[index].equals("\"SL.AGR.EMPL.FE.ZS\""))
		{
			rowCode = rowStr[1].substring(1, rowStr[1].length() - 1) + " AGR";
		}
		else if (rowStr[index].equals("\"SL.IND.EMPL.FE.ZS\""))
		{
			rowCode = rowStr[1].substring(1, rowStr[1].length() - 1) + " IND";
		}
		else if (rowStr[index].equals("\"SL.SRV.EMPL.FE.ZS\""))
		{
			rowCode = rowStr[1].substring(1, rowStr[1].length() - 1) + " SRV";
		}
		
		if (!rowCode.isEmpty())
		{
			int index2000 = 44;
			
			Float valueLeftMostYear = 0.0F, valueRightMostYear = 0.0F;
			for (int x = index2000; x < 59; x++){
				try
				{
					valueLeftMostYear = Float.parseFloat(rowStr[x].substring(1, rowStr[x].length() - 1));
				}
				catch(NumberFormatException nfe)
				{
					continue;
				}
				break;
			}
			
			//prevent NaN values being produced from divide by zero in reducer
			if (valueLeftMostYear == 0.0)
			{
				return;
			}
			
			int index2016 = 59;
			for(int x = index2016; x > index2000; x--)
			{
				try
				{
					valueRightMostYear = Float.parseFloat(rowStr[x].substring(1, rowStr[x].length() - 1));
				}
				catch(NumberFormatException nfe)
				{
					continue;
				}
				break;
				
			}
				
			context.write(new Text(rowCode), new FloatWritable(valueLeftMostYear));
			context.write(new Text(rowCode), new FloatWritable(valueRightMostYear));
			
		}
		
	}
	
}
