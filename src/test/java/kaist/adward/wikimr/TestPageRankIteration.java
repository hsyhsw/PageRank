package kaist.adward.wikimr;

import kaist.adward.wikimr.mapper.PageRankIterationMapper;
import kaist.adward.wikimr.reducer.PageRankIterationReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestPageRankIteration {

	MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
	ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

	@Before
	public void setup() {
		PageRankIterationMapper mapper = new PageRankIterationMapper();
		PageRankIterationReducer reducer = new PageRankIterationReducer();
		mapDriver = new MapDriver<LongWritable, Text, IntWritable, Text>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<IntWritable, Text, IntWritable, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.addInput(new LongWritable(1), new Text("1\t1.0\t2"));
		mapDriver.addInput(new LongWritable(2), new Text("2\t1.0\t1"));
		mapDriver.addInput(new LongWritable(3), new Text("3\t1.0\t2"));
		mapDriver.addOutput(new IntWritable(1), new Text("1.0\t2"));
		mapDriver.addOutput(new IntWritable(2), new Text("1.0"));
		mapDriver.addOutput(new IntWritable(2), new Text("1.0\t1"));
		mapDriver.addOutput(new IntWritable(1), new Text("1.0"));
		mapDriver.addOutput(new IntWritable(3), new Text("1.0\t2"));
		mapDriver.addOutput(new IntWritable(2), new Text("1.0"));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text("1.0"));
		values1.add(new Text("1.0\t2"));
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text("1.0"));
		values2.add(new Text("1.0\t1"));
		List<Text> values3 = new ArrayList<Text>();
		values3.add(new Text("1.0\t2"));

		reduceDriver.addInput(new IntWritable(1), values1);
		reduceDriver.addInput(new IntWritable(2), values2);
		reduceDriver.addInput(new IntWritable(3), values3);
		reduceDriver.addOutput(new IntWritable(1), new Text("1.0\t2"));
		reduceDriver.addOutput(new IntWritable(2), new Text("1.0\t1"));
		reduceDriver.addOutput(new IntWritable(3), new Text("0.14999998\t2"));

		reduceDriver.runTest();
	}


	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.addInput(new LongWritable(1), new Text("1\t0.575\t2"));
		mapReduceDriver.addInput(new LongWritable(2), new Text("2\t1.85\t1"));
		mapReduceDriver.addInput(new LongWritable(3), new Text("3\t0.14999998\t2"));
		mapReduceDriver.addOutput(new IntWritable(1), new Text("1.7225001\t2"));
		mapReduceDriver.addOutput(new IntWritable(2), new Text("0.76624995\t1"));
		mapReduceDriver.addOutput(new IntWritable(3), new Text("0.14999998\t2"));

		mapReduceDriver.runTest();
	}
}