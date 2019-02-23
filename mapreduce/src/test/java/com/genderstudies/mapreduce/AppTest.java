package com.genderstudies.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.BeforeClass;
import org.junit.Test;

import com.genderstudies.mapper.Q1Mapper;
import com.genderstudies.mapper.Q2Mapper;
import com.genderstudies.reduce.Q1Reducer;
import com.genderstudies.reduce.Q2Reducer;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
	
	private static MapDriver<LongWritable, Text, Text, FloatWritable> Q1MapDrive;
	private static MapDriver<LongWritable, Text, Text, FloatWritable> Q2MapDrive;
	
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> Q1ReduceDrive;
	
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> Q2ReduceDrive;
	
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> Q1mapReduceDriver;
	
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> Q2mapReduceDriver;
	
	@BeforeClass
	public static void Setup()
	{
		//q1mapper
		Q1MapDrive = new MapDriver();
		Q1Mapper q1mapper = new Q1Mapper();
		Q1MapDrive.setMapper(q1mapper);
		
		//q1reducer
		Q1ReduceDrive = new ReduceDriver();
		Q1Reducer q1reducer = new Q1Reducer();
		Q1ReduceDrive.setReducer(q1reducer);
		
		//Q1MapReduceDriver
		Q1mapReduceDriver = new MapReduceDriver();
		Q1mapReduceDriver.setMapper(q1mapper);
		Q1mapReduceDriver.setReducer(q1reducer);
		
		//q2mapper
		Q2MapDrive = new MapDriver();
		Q2Mapper q2mapper = new Q2Mapper();
		Q2MapDrive.setMapper(q2mapper);
		
		//q2reducer
		Q2ReduceDrive = new ReduceDriver();
		Q2Reducer q2reducer = new Q2Reducer();
		Q2ReduceDrive.setReducer(q2reducer);
		
		//Q2MapReduceDriver
		Q2mapReduceDriver = new MapReduceDriver();
		Q2mapReduceDriver.setMapper(q2mapper);
		Q2mapReduceDriver.setReducer(q2reducer);
		
		
	}
	//For Q1's mapper...
	@Test
	public void TestMapExampleRow()
	{
		
		Q1MapDrive.withInput(new LongWritable(1), 
				new Text("\"test\",\"TST\",\"plz work\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.37\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",") );
		
		Q1MapDrive.withOutput(new Text("TST"), new FloatWritable(13.37F));
		
		Q1MapDrive.runTest();
	}
	//For Q1's reducer...
	@Test
	public void TestReduceExampleRow()
	{
		ArrayList<FloatWritable> testList = new ArrayList<FloatWritable>();
		
		testList.add(new FloatWritable(13.37F));
		
		Q1ReduceDrive.withInput(new Text("test"), testList);
		
		Q1ReduceDrive.withOutput(new Text("test"), new FloatWritable(13.37F));
		
		Q1ReduceDrive.runTest();
		
	}
	@Test
	public void TestQ1MapReduceRow()
	{
		Q1mapReduceDriver.withInput(new LongWritable(1), 
		new Text("\"test\",\"TST\",\"plz work\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.37\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",") );
		
		Q1mapReduceDriver.withOutput(new Text("TST"), new FloatWritable(13.37F));
		
		Q1mapReduceDriver.runTest();
		
	}
	
	@Test
	public void TestQ2MapperExampleRow()
	{
		
		Q2MapDrive.withInput(new LongWritable(1), 
		new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"40.75\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		Q2MapDrive.withOutput(new Text("USA"), new FloatWritable(35.37453F));
		Q2MapDrive.withOutput(new Text("USA"), new FloatWritable(36.00504F));
		Q2MapDrive.withOutput(new Text("USA"), new FloatWritable(40.75F));
		Q2MapDrive.runTest();

	}
	@Test
	public void TestQ2ReducerExampleRow()
	{
		List<FloatWritable> testList = new ArrayList<FloatWritable>();
		
		testList.add(new FloatWritable(35.37453F));
		
		testList.add(new FloatWritable(36.00504F));
		
		testList.add(new FloatWritable(40.75F));
		
		Q2ReduceDrive.withInput(new Text("USA"), testList);
		
		Float output = (Math.abs(35.37453F - 36.00504F) + Math.abs(36.00504F - 40.75F))/2;
		
		Q2ReduceDrive.withOutput(new Text("USA"), new FloatWritable(output));
		Q2ReduceDrive.runTest();
	}
	
	@Test
	public void TestQ2MapReduceCombo()
	{
		Q2mapReduceDriver.withInput(new LongWritable(1), 
		new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"40.75\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		Float output = (Math.abs(35.37453F - 36.00504F) + Math.abs(36.00504F - 40.75F))/2;
		
		Q2mapReduceDriver.withOutput(new Text("USA"), new FloatWritable(output));
		Q2mapReduceDriver.runTest();
	}
	
	
	
	
}
