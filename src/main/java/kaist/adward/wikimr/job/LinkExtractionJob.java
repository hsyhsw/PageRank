package kaist.adward.wikimr.job;

import kaist.adward.wikimr.mapper.LinkExtractionMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LinkExtractionJob {
	/**
	 * Parses input dump files and extracts links for all pages
	 *
	 * @param inputPath  Raw wikipedia input folder path
	 * @param outputPath Extracted links output folder path
	 * @throws java.io.IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public void extractLinks(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Extract Links from Wikipedia Dumps");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(LinkExtractionMapper.class);

		// IdentityReducer is the default implementation of Reducer class
		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(PageRank.class);
		job.waitForCompletion(true);
	}
}
