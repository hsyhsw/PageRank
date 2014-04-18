package kaist.adward.wikimr.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class PageRankIterationReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	private static final double damping = 0.85D;
	double newPageRank;

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		double sumDeltaPageRanks = 0.0D;
		double deltaPageRank;
		String links = null;

		while (itr.hasNext()) {
			String record = itr.next().toString();

			try {
				deltaPageRank = Double.parseDouble(record);
				sumDeltaPageRanks += deltaPageRank;
			} catch (NumberFormatException e) {
				// the record must be original list of links
				int linksStartingIndex = record.indexOf("\t") + 1;
				links = record.substring(linksStartingIndex, record.length());
			}
		}

		newPageRank = damping * sumDeltaPageRanks + (1 - damping);

		// when there are no links, it means the key is not directing to a wiki page with valid id
		if (links != null) {
			context.write(key, new Text(newPageRank + "\t" + links));
		}
	}
}