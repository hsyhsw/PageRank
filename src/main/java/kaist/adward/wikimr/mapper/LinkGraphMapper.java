package kaist.adward.wikimr.mapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;

public class LinkGraphMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private HashMap<String, Integer> lookupTable;

	@Override
	protected void setup(Context context) throws IOException {
		lookupTable = new HashMap<String, Integer>();

		URI[] caches = context.getCacheArchives();
		FileSystem fs = FileSystem.get(caches[0], context.getConfiguration());

		for (URI cache : caches) {
			InputStream is = fs.open(new Path(cache));
			setupLookupTable(is);
			is.close();
		}
	}

	/**
	 * read reverse lookup table from distributed cache
	 *
	 * @param inputStream input stream for a reverse lookup table distributed cache file
	 */
	private void setupLookupTable(InputStream inputStream) {
		Scanner scanner = new Scanner(inputStream);
		while (scanner.hasNextLine()) {
			// assume the excluded words are delimited by a comma
			// ignore character case
			String line = scanner.nextLine();
			String[] fields = line.split("\t");
			Integer docId = Integer.parseInt(fields[0]);
			String title = fields[1];
			lookupTable.put(title, docId);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		String[] fields = record.split("\t", -1);
		Integer docId = Integer.parseInt(fields[0].split("\\|", -1)[0]);

		StringBuilder linkBuilder = new StringBuilder();
		boolean isFirst = true;
		for (int i = 2; i < fields.length; i++) {
			String link = fields[i];
			Integer linkDocId = lookupTable.get(link);

			if (linkDocId != null) {
				if (!isFirst) {
					linkBuilder.append("\t");
				}
				// we found a matching document id
				linkBuilder.append(linkDocId);
				isFirst = false;
			}
		}

		context.write(new IntWritable(docId), new Text("1.0\t" + linkBuilder.toString()));
	}
}