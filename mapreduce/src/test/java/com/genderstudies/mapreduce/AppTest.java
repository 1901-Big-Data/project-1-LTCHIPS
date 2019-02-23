package com.genderstudies.mapreduce;

import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.BeforeClass;
import org.junit.Test;

import com.genderstudies.mapper.Q1Mapper;
import com.genderstudies.reduce.Q1Reducer;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
	
	private static MapDriver<LongWritable, Text, Text, FloatWritable> mapDrive;
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDrive;
	
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDrive;
	
	@BeforeClass
	public static void Setup()
	{
		//Mapper
		mapDrive = new MapDriver();
		Q1Mapper mapper = new Q1Mapper();
		mapDrive.setMapper(mapper);
		
		//TODO: a REDUCER...
		reduceDrive = new ReduceDriver();
		Q1Reducer reducer = new Q1Reducer();
		reduceDrive.setReducer(reducer);
		
		//MapReduceDriver
		mapReduceDrive = new MapReduceDriver();
		mapReduceDrive.setMapper(mapper);
		mapReduceDrive.setReducer(reducer);
		
	}
	//For Q1's mapper...
	@Test
	public void TestMapExampleRow()
	{
		
		mapDrive.withInput(new LongWritable(1), 
				new Text("\"test\",\"TST\",\"plz work\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.37\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",") );
		
		mapDrive.withOutput(new Text("TST"), new FloatWritable(13.37F));
		
		mapDrive.runTest();
	}
	//For Q1's reducer...
	@Test
	public void TestReduceExampleRow()
	{
		ArrayList<FloatWritable> testList = new ArrayList<FloatWritable>();
		
		testList.add(new FloatWritable(13.37F));
		
		reduceDrive.withInput(new Text("test"), testList);
		
		reduceDrive.withOutput(new Text("test"), new FloatWritable(13.37F));
		
		reduceDrive.runTest();
		
	}
	@Test
	public void TestQ1MapReduceRow()
	{
		mapReduceDrive.withInput(new LongWritable(1), 
		new Text("\"test\",\"TST\",\"plz work\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.37\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",") );
		
		mapReduceDrive.withOutput(new Text("TST"), new FloatWritable(13.37F));
		
		mapReduceDrive.runTest();
		
	}
}
