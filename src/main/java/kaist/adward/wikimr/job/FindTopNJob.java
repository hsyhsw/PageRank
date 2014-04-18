package kaist.adward.wikimr.job;

import kaist.adward.wikimr.mapper.TopNMapper;
import kaist.adward.wikimr.reducer.TopNReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FindTopNJob {
	/**
	 * Chooses top N pages with higher PageRanks.
	 *
	 * @param inputPath  Input folder path which is from the final iteration of pagerank
	 * @param outputPath Final output folder path
	 * @param cachePath  Lookup table folder path
	 * @param N          How many top results you want on the final output, which comes from command arguments
	 * @throws java.io.IOException
	 */
	public void queryTopNPageRanks(String inputPath, String outputPath, String cachePath, int N)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Top N Page Ranks");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().set("N", Integer.toString(N));

		// add distributed cache
		System.err.println("CachePath: " + cachePath);
		cachePath = cachePath.replaceFirst("s3://", "s3n://"); // access s3 native file system
		Path path = new Path(cachePath);
		FileSystem fs = FileSystem.get(path.toUri(), job.getConfiguration());
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus file : files) {
			job.addCacheArchive(file.getPath().toUri());
		}

		// we must have single reducer to finalize top N
		job.setNumReduceTasks(1);

		job.setJarByClass(PageRank.class);
		job.waitForCompletion(true);
	}
}
