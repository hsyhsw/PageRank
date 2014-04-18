package kaist.adward.wikimr.job;

import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Calculates PageRank of articles from a snapshot of English Wikipedia dumps.
 */
public class PageRank {
	private static final Logger logger = Logger.getLogger(PageRank.class);
	private static final NumberFormat twoDigits = new DecimalFormat("00");

	public static void main(String[] args) throws Exception {

		if (args.length != 5) {
			System.err.println("Usage: PageRank <input path> <output path> <number of pagerank calculation iterations> <N> <job # to start from(1~5)>");
			System.exit(-1);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int iterations = Integer.valueOf(args[2]);
		int N = Integer.valueOf(args[3]);
		int startJobNumber = Integer.valueOf(args[4]);

		// stopwatch from apache commons - let's see how long it would take to process the whole thing
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		// Job 1: parse XML and extract links from all pages
		if (startJobNumber < 2) {
			LinkExtractionJob linkExtractionJob = new LinkExtractionJob();
			linkExtractionJob.extractLinks(inputPath, outputPath + "/links");
		}

		// Job 2: Generate lookup table for page id/title, which would be used as distributed cache
		if (startJobNumber < 3) {
			LookupTableGenerationJob lookupTableGenerationJob = new LookupTableGenerationJob();
			lookupTableGenerationJob.generate(outputPath + "/links", outputPath + "/lookup-table");
		}

		// Job 3: Generate link graph of page ids and remove link pages that cannot be identified with id
		if (startJobNumber < 4) {
			LinkGraphGenerationJob linkGraphGenerationJob = new LinkGraphGenerationJob();
			linkGraphGenerationJob.generate(outputPath + "/links", outputPath + "/iteration-00", outputPath + "/lookup-table/");
		}

		// Job 4: calculate PageRank (iterative)
		if (startJobNumber < 5) {
			PageRankCalculationJob pageRankCalculationJob = new PageRankCalculationJob();
			for (int i = 0; i < iterations; i++) {
				pageRankCalculationJob.calculatePageRank(outputPath + "/iteration-" + twoDigits.format(i), outputPath + "/iteration-" + twoDigits.format(i + 1));
			}
		}

		// Job 5: sort top N by PageRank
		if (startJobNumber < 6) {
			FindTopNJob findTopNJob = new FindTopNJob();
			findTopNJob.queryTopNPageRanks(outputPath + "/iteration-" + twoDigits.format(iterations), outputPath + "/top-" + N + "-pagerank", outputPath + "/lookup-table/", N);
		}

		stopWatch.stop();
		logger.info("Completed Jobs in " + stopWatch.getTime() / 1000.0 + " seconds.");
		stopWatch.reset();
	}
}