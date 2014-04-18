package kaist.adward.wikimr.reducer;

import kaist.adward.wikimr.util.TreeMultiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	private static final TreeMultiMap<Double, Text> topN = new TreeMultiMap<Double, Text>();
	private static int N;
	private static HashMap<Integer, String> lookupTable;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		N = Integer.parseInt(conf.get("N"));
	}

	/**
	 * read reverse lookup table from distributed cache
	 *
	 * @param inputStream input stream for a reverse lookup table distributed cache file
	 */
	private void setupLookupTable(InputStream inputStream) {
		Scanner scanner = new Scanner(inputStream);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] fields = line.split("\t");
			Integer docId = Integer.parseInt(fields[0]);
			String title = fields[1];
			lookupTable.put(docId, title);
		}
	}

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
		// value is in the form of <document id, PageRank>
		for (Text record : values) {
			String[] fields = record.toString().split("\t", -1);
			double pageRank = Double.parseDouble(fields[1]);

			topN.put(pageRank, new Text(record));

			// if we have more than N records in top N now, remove the lowest
			if (topN.size() > N) {
				topN.remove(topN.firstKey());
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// there is a bug in hadoop 2.2 with context.getCacheFiles() method due to typo.
		// when called from reducer (if called from mapper, it works fine) it returns null.
		// this bug is fixed in hadoop 2.3, but we don't have 2.3 on EMR yet.
		// so we use getCacheArchives() instead.
		lookupTable = new HashMap<Integer, String>();

		URI[] caches = context.getCacheArchives();
		FileSystem fs = FileSystem.get(caches[0], context.getConfiguration());

		for (URI cache : caches) {
			InputStream is = fs.open(new Path(cache));
			setupLookupTable(is);
			is.close();
		}

		// transform document id to title using lookup table
		for (Text record : topN.descendingMap().values()) {
			String[] fields = record.toString().split("\t", -1);
			Integer docId = Integer.parseInt(fields[0]);
			String pageRank = fields[1];
			String title = lookupTable.get(docId);

			context.write(NullWritable.get(), new Text(title + "|" + pageRank));
		}
	}
}