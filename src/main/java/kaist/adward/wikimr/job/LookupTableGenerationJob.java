package kaist.adward.wikimr.job;

import kaist.adward.wikimr.mapper.LookupTableMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LookupTableGenerationJob {
	/**
	 * Generates <id,title> pair of all pages from the input.
	 *
	 * @param inputPath  Extracted links input folder path
	 * @param outputPath Lookup table output folder path
	 * @throws java.io.IOException
	 */
	public void generate(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Generate Lookup Table");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(LookupTableMapper.class);

		// IdentityReducer is the default implementation of Reducer class
		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(PageRank.class);
		job.waitForCompletion(true);
	}
}
