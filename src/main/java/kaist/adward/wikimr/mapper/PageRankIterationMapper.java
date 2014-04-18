package kaist.adward.wikimr.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankIterationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		String[] fields = record.split("\t", -1);
		int docId = Integer.valueOf(fields[0]);
		double currentPageRank = Double.valueOf(fields[1]);
		double delta = 0.0D;
		int totalOutLinks = fields.length - 2;

		if (totalOutLinks > 0) {
			delta = currentPageRank / (double) totalOutLinks;
		}

		int valueStartingIndex = record.indexOf(fields[0]) + fields[0].length() + 1;
		String originalValue = record.substring(valueStartingIndex, record.length());
		// emit original input for next iteration data
		// we just add pageRank to identify original input
		context.write(new IntWritable(docId), new Text(originalValue));

		for (int i = 2; i < fields.length; i++) {
			if (fields[i].equals(""))
				continue;

			int link = Integer.valueOf(fields[i]);
			// emit (link, delta in pagerank) if the link is not self-referenced
			if (link != docId) {
				context.write(new IntWritable(link), new Text(Double.toString(delta)));
			}
		}
	}
}