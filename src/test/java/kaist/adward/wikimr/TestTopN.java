package kaist.adward.wikimr;

import kaist.adward.wikimr.mapper.TopNMapper;
import kaist.adward.wikimr.reducer.TopNReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

public class TestTopN {

	MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
	ReduceDriver<NullWritable, Text, NullWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, NullWritable, Text, NullWritable, Text> mapReduceDriver;
	Configuration conf;

	@Before
	public void setup() throws IOException, URISyntaxException {
		TopNMapper mapper = new TopNMapper();
		TopNReducer reducer = new TopNReducer();
		mapDriver = new MapDriver<LongWritable, Text, NullWritable, Text>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<NullWritable, Text, NullWritable, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, NullWritable, Text, NullWritable, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);

		mapDriver.getContext().getConfiguration().set("N", "3");
		reduceDriver.getContext().getConfiguration().set("N", "3");
		mapReduceDriver.getConfiguration().set("N", "3");

		when(reduceDriver.getContext().getNumReduceTasks()).thenReturn(1);

		// prepare test lookup table
		String lookupTableInputFile = "/test-lookup-table.txt";
		URI[] caches = new URI[1];
		caches[0] = this.getClass().getResource(lookupTableInputFile).toURI();
		when(reduceDriver.getContext().getCacheArchives()).thenReturn(caches);
		mapReduceDriver.setCacheArchives(caches);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.addInput(new LongWritable(1), new Text("1\t1906.2871"));
		mapDriver.addInput(new LongWritable(2), new Text("2\t1295.7625"));
		mapDriver.addInput(new LongWritable(3), new Text("3\t1521.7087"));
		mapDriver.addInput(new LongWritable(4), new Text("4\t1041.3579"));
		mapDriver.addInput(new LongWritable(5), new Text("5\t350.08752"));
		mapDriver.addInput(new LongWritable(6), new Text("6\t2655.2842"));
		mapDriver.addInput(new LongWritable(7), new Text("7\t8016.9395"));

		mapDriver.addOutput(NullWritable.get(), new Text("7\t8016.9395"));
		mapDriver.addOutput(NullWritable.get(), new Text("6\t2655.2842"));
		mapDriver.addOutput(NullWritable.get(), new Text("1\t1906.2871"));

		mapDriver.runTest(false);
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1\t1906.2871"));
		values.add(new Text("2\t1295.7625"));
		values.add(new Text("3\t1521.7087"));
		values.add(new Text("4\t1041.3579"));
		values.add(new Text("5\t350.08752"));
		values.add(new Text("6\t2655.2842"));
		values.add(new Text("7\t8016.9395"));

		reduceDriver.addInput(NullWritable.get(), values);

		reduceDriver.addOutput(NullWritable.get(), new Text("United States\t8016.9395"));
		reduceDriver.addOutput(NullWritable.get(), new Text("United Kingdom\t2655.2842"));
		reduceDriver.addOutput(NullWritable.get(), new Text("Germany\t1906.2871"));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.addInput(new LongWritable(1), new Text("1\t1906.2871"));
		mapReduceDriver.addInput(new LongWritable(2), new Text("2\t1295.7625"));
		mapReduceDriver.addInput(new LongWritable(3), new Text("3\t1521.7087"));
		mapReduceDriver.addInput(new LongWritable(4), new Text("4\t1041.3579"));
		mapReduceDriver.addInput(new LongWritable(5), new Text("5\t350.08752"));
		mapReduceDriver.addInput(new LongWritable(6), new Text("6\t2655.2842"));
		mapReduceDriver.addInput(new LongWritable(7), new Text("7\t8016.9395"));

		mapReduceDriver.addOutput(NullWritable.get(), new Text("United States\t8016.9395"));
		mapReduceDriver.addOutput(NullWritable.get(), new Text("United Kingdom\t2655.2842"));
		mapReduceDriver.addOutput(NullWritable.get(), new Text("Germany\t1906.2871"));

		mapReduceDriver.runTest();
	}
}
