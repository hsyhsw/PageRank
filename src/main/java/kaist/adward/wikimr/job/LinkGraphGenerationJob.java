package kaist.adward.wikimr.job;

import kaist.adward.wikimr.mapper.LinkGraphMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LinkGraphGenerationJob {
	/**
	 * Generate link graph of page ids and remove link pages that cannot be identified with id.
	 *
	 * @param inputPath  Extracted links input folder path
	 * @param outputPath Page ids link graph output folder path
	 * @param cachePath  Lookup table folder path
	 * @throws java.io.IOException
	 */
	public void generate(String inputPath, String outputPath, String cachePath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Generate Link Graph");

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(LinkGraphMapper.class);
		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// add distributed cache
		System.err.println("CachePath: " + cachePath);
		cachePath = cachePath.replaceFirst("s3://", "s3n://"); // access s3 native file system
		Path path = new Path(cachePath);
		FileSystem fs = FileSystem.get(path.toUri(), job.getConfiguration());
		FileStatus[] files = fs.listStatus(path);
		for (FileStatus file : files) {
			job.addCacheArchive(file.getPath().toUri());
		}

		job.setJarByClass(PageRank.class);
		job.waitForCompletion(true);
	}
}