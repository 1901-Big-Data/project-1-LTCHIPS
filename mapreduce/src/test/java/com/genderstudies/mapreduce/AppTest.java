package com.genderstudies.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
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
import com.genderstudies.mapper.Q3Mapper;
import com.genderstudies.mapper.Q4Mapper;
import com.genderstudies.mapper.Q5Mapper;
import com.genderstudies.reduce.Q2Reducer;
import com.genderstudies.reduce.Q1Reducer;
import com.genderstudies.reduce.Q3Reducer;
import com.genderstudies.reduce.Q5Reducer;

/**
 * Unit tests for super duper complex app
 */
public class AppTest 
{
	
	private static MapDriver<LongWritable, Text, Text, FloatWritable> Q1MapDrive;
	private static MapDriver<LongWritable, Text, Text, FloatWritable> Q2MapDrive;
	private static MapDriver<LongWritable, Text, Text, FloatWritable> Q3MapDrive;
	private static MapDriver<LongWritable, Text, Text, FloatWritable> Q4MapDrive;
	private static MapDriver<LongWritable, Text, Text, DoubleWritable> Q5MapDrive;
	
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> Q1ReduceDrive;
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> Q2ReduceDrive;
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> Q3ReduceDrive;
	private static ReduceDriver<Text, FloatWritable, Text, FloatWritable> Q4ReduceDrive;
	private static ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> Q5ReduceDrive;
	
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> Q1mapReduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> Q2mapReduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> Q3mapReduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> Q4mapReduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> Q5mapReduceDriver;
	
	
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
		
		
		//q3mapper
		Q3MapDrive = new MapDriver();
		Q3Mapper q3mapper = new Q3Mapper();
		Q3MapDrive.setMapper(q3mapper);
		
		Q3ReduceDrive = new ReduceDriver();
		Q3Reducer q3reducer = new Q3Reducer();
		Q3ReduceDrive.setReducer(q3reducer);
		
		//Q3MapReduceDriver
		Q3mapReduceDriver = new MapReduceDriver();
		Q3mapReduceDriver.setMapper(q3mapper);
		Q3mapReduceDriver.setReducer(q3reducer);
		
		//q4mapper
		Q4MapDrive = new MapDriver();
		Q4Mapper q4mapper = new Q4Mapper();
		Q4MapDrive.setMapper(q4mapper);
		
		Q4ReduceDrive = new ReduceDriver();
		Q4ReduceDrive.setReducer(q3reducer);
		
		Q4mapReduceDriver = new MapReduceDriver();
		Q4mapReduceDriver.setMapper(q4mapper);
		Q4mapReduceDriver.setReducer(q3reducer);
		
		//q5mapper
		Q5MapDrive = new MapDriver();
		Q5Mapper q5mapper = new Q5Mapper();
		Q5MapDrive.setMapper(q5mapper);
		
		Q5ReduceDrive = new ReduceDriver();
		Q5Reducer q5reducer = new Q5Reducer();
		Q5ReduceDrive.setReducer(q5reducer);
		
		Q5mapReduceDriver = new MapReduceDriver();
		Q5mapReduceDriver.setMapper(q5mapper);
		Q5mapReduceDriver.setReducer(q5reducer);
		
		
	}
	//For Q1's mapper...
	@Test
	public void TestMapExampleRow()
	{	
		Q1MapDrive.withInput(new LongWritable(1), 
				new Text("\"test\",\"TST\",\"plz work\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.37\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",") );
		
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
		new Text("\"test\",\"TST\",\"plz work\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.37\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",") );
		
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
		Q2MapDrive.runTest(false);
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
	
	@Test
	public void TestQ3ARGMapper()
	{
		Q3MapDrive.resetOutput();
		
		Q3MapDrive.withInput(new LongWritable(1),
				new Text("\"test\",\"tst\",\"kek\",\"SL.AGR.EMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q3MapDrive.withOutput(new Text("tst AGR"), new FloatWritable(0.949999988F));
		Q3MapDrive.withOutput(new Text("tst AGR"), new FloatWritable(0.930000007F));
		
		Q3MapDrive.runTest(false);
	}
	
	@Test
	public void TestQ3INDMapper()
	{
		Q3MapDrive.resetOutput();
		
		Q3MapDrive.withInput(new LongWritable(2),
				new Text("\"test\",\"tst\",\"kek\",\"SL.IND.EMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q3MapDrive.withOutput(new Text("tst IND"), new FloatWritable(0.949999988F));
		Q3MapDrive.withOutput(new Text("tst IND"), new FloatWritable(0.930000007F));
		
		Q3MapDrive.runTest(false);
		
	}
	
	@Test
	public void TestQ3SRVMapper()
	{
		Q3MapDrive.resetOutput();
		
		Q3MapDrive.withInput(new LongWritable(3),
				new Text("\"test\",\"tst\",\"kek\",\"SL.SRV.EMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q3MapDrive.withOutput(new Text("tst SRV"), new FloatWritable(0.949999988F));
		Q3MapDrive.withOutput(new Text("tst SRV"), new FloatWritable(0.930000007F));
		
		Q3MapDrive.runTest(false);
	}
	
	@Test
	public void TestQ3Reducer()
	{
		List<FloatWritable> testList = new ArrayList<FloatWritable>();
		
		testList.add(new FloatWritable(0.949999988F));
		
		testList.add(new FloatWritable(0.930000007F));
		
		Q3ReduceDrive.withInput(new Text("tst SRV"), testList);
		
		Q3ReduceDrive.withOutput(new Text("tst SRV"), new FloatWritable(-2.105261F));
		
		Q3ReduceDrive.runTest();
		
	}
	
	@Test
	public void TestQ3MapperAndReducer()
	{
		Q3mapReduceDriver.withInput(new LongWritable(1),
				new Text("\"test\",\"tst\",\"kek\",\"SL.AGR.EMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q3mapReduceDriver.withInput(new LongWritable(2),
				new Text("\"test\",\"tst\",\"kek\",\"SL.IND.EMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q3mapReduceDriver.withInput(new LongWritable(3),
				new Text("\"test\",\"tst\",\"kek\",\"SL.SRV.EMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q3mapReduceDriver.withOutput(new Text("tst AGR"), new FloatWritable(-2.105261F));
		
		Q3mapReduceDriver.withOutput(new Text("tst IND"), new FloatWritable(-2.105261F));
		
		Q3mapReduceDriver.withOutput(new Text("tst SRV"), new FloatWritable(-2.105261F));
		
		Q3mapReduceDriver.runTest(false);
		
	}
	
	@Test
	public void TestQ4ARGMapper()
	{
		Q4MapDrive.resetOutput();
		
		Q4MapDrive.withInput(new LongWritable(1),
				new Text("\"test\",\"tst\",\"kek\",\"SL.AGR.EMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q4MapDrive.withOutput(new Text("tst AGR"), new FloatWritable(0.949999988F));
		Q4MapDrive.withOutput(new Text("tst AGR"), new FloatWritable(0.930000007F));
		
		Q4MapDrive.runTest(false);
	}
	
	@Test
	public void TestQ4INDMapper()
	{
		Q4MapDrive.resetOutput();
		
		Q4MapDrive.withInput(new LongWritable(2),
				new Text("\"test\",\"tst\",\"kek\",\"SL.IND.EMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q4MapDrive.withOutput(new Text("tst IND"), new FloatWritable(0.949999988F));
		Q4MapDrive.withOutput(new Text("tst IND"), new FloatWritable(0.930000007F));
		
		Q4MapDrive.runTest(false);
		
	}
	
	@Test
	public void TestQ4SRVMapper()
	{
		Q4MapDrive.resetOutput();
		
		Q4MapDrive.withInput(new LongWritable(3),
				new Text("\"test\",\"tst\",\"kek\",\"SL.SRV.EMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q4MapDrive.withOutput(new Text("tst SRV"), new FloatWritable(0.949999988F));
		Q4MapDrive.withOutput(new Text("tst SRV"), new FloatWritable(0.930000007F));
		
		Q4MapDrive.runTest(false);
	}
	
	@Test
	public void TestQ4Reducer()
	{
		List<FloatWritable> testList = new ArrayList<FloatWritable>();
		
		testList.add(new FloatWritable(0.949999988F));
		
		testList.add(new FloatWritable(0.930000007F));
		
		Q4ReduceDrive.withInput(new Text("tst SRV"), testList);
		
		Q4ReduceDrive.withOutput(new Text("tst SRV"), new FloatWritable(-2.105261F));
		
		Q4ReduceDrive.runTest();
		
	}
	
	@Test
	public void TestQ4MapperAndReducer()
	{
		Q4mapReduceDriver.withInput(new LongWritable(1),
				new Text("\"test\",\"tst\",\"kek\",\"SL.AGR.EMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q4mapReduceDriver.withInput(new LongWritable(2),
				new Text("\"test\",\"tst\",\"kek\",\"SL.IND.EMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q4mapReduceDriver.withInput(new LongWritable(3),
				new Text("\"test\",\"tst\",\"kek\",\"SL.SRV.EMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.949999988\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.930000007\","));
		
		Q4mapReduceDriver.withOutput(new Text("tst AGR"), new FloatWritable(-2.105261F));
		
		Q4mapReduceDriver.withOutput(new Text("tst IND"), new FloatWritable(-2.105261F));
		
		Q4mapReduceDriver.withOutput(new Text("tst SRV"), new FloatWritable(-2.105261F));
		
		Q4mapReduceDriver.runTest(false);
		
	}
	
	@Test
	public void TestQ5MapperExampleRow()
	{
		Q5MapDrive.resetOutput();
		
		Q5MapDrive.withInput(new LongWritable(1), 
			new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\","));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(35.37453));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(36.00504));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(37.52263));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(-1.0));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(38.44067));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(39.15297));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(39.89922));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(40.53132));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(41.12231));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(20.18248));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(20.38445));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(20.68499));
		Q5MapDrive.withOutput(new Text("USA"), new DoubleWritable(-1.0));
		
		
		Q5MapDrive.runTest(false);
	}
	
	@Test
	public void TestQ5ReducerExampleRow()
	{
		Q5ReduceDrive.resetOutput();
		
		List<DoubleWritable> testList = new ArrayList<DoubleWritable>();
		testList.add( new DoubleWritable(35.37453));
		testList.add( new DoubleWritable(36.00504));
		testList.add( new DoubleWritable(37.52263));
		testList.add( new DoubleWritable(-1.0));
		testList.add( new DoubleWritable(38.44067));
		testList.add( new DoubleWritable(39.15297));
		testList.add( new DoubleWritable(39.89922));
		testList.add( new DoubleWritable(40.53132));
		testList.add( new DoubleWritable(41.12231));
		testList.add( new DoubleWritable(20.18248));
		testList.add(new DoubleWritable(20.38445));
		testList.add(new DoubleWritable(20.68499));
		testList.add( new DoubleWritable(-1.0));
		
		Q5ReduceDrive.withInput(new Text("USA"), testList);
		
		Double output = (41.12231 - 20.18248);
		
		Q5ReduceDrive.withOutput(new Text("USA (2012-2013)"), new DoubleWritable(output));
		Q5ReduceDrive.runTest(false);
	}
	
	@Test
	public void TestQ5MapReduceCombo()
	{
		Q5mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\","));

		Double output = (41.12231 - 20.18248);
		
		Q5mapReduceDriver.withOutput(new Text("USA (2012-2013)"), new DoubleWritable(output));
		Q5mapReduceDriver.runTest();
	}
}
