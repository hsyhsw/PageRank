package kaist.adward.wikimr.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LookupTableMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	/**
	 * Extracts only id and title of the document
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String record = line.split("\t", -1)[0];
		String[] fields = record.split("\\|", -1);
		int docId = Integer.valueOf(fields[0]);
		String title = fields[1];

		context.write(new IntWritable(docId), new Text(title));
	}
}