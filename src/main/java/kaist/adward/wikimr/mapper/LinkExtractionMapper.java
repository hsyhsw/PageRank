package kaist.adward.wikimr.mapper;

import kaist.adward.wikimr.model.WikiPage;
import kaist.adward.wikimr.util.WikiTextParser;
import kaist.adward.wikimr.util.WikiXmlSAXParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinkExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Pattern garbageTitles = Pattern.compile("^\\w+:");

	@Override
	/**
	 * Converts XML representation of a page into original document id|title and a list of its links.
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String page = value.toString();
		WikiPage wikiPage = parseXml(page);

		String wikiText = wikiPage.getWikiText();
		Long docId = wikiPage.getDocumentId();
		String title = wikiPage.getTitle();

		Matcher matcher = garbageTitles.matcher(title);
		if (matcher.find())
			return;
		if (!title.matches("[\\x20-\\x7F]+"))
			return;
		if (wikiPage.getNs() != 0)
			return;

		List<String> links = null;

		try {
			links = WikiTextParser.getInstance().parseLinks(wikiText);
		} catch (StackOverflowError e) {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			System.out.println(filename + ":");
			System.out.println("\tParsing error(stack ovfl): " + docId + ", " + wikiPage.getTitle());
			return;
		} catch (Exception e) {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			System.out.println(filename + ":");
			System.out.println("\tParsing error: " + docId + ", " + wikiPage.getTitle());
			e.printStackTrace(System.out);
			return;
		}

		boolean firstValue = true;
		StringBuilder strLinks = new StringBuilder();

		for (String link : links) {
			if (!firstValue && !link.equals("")) {
				strLinks.append("\t");
			}
			firstValue = false;
			strLinks.append(link.trim());
		}

		context.write(new Text(docId + "|" + title), new Text("1.0\t" + strLinks.toString()));
		context.getCounter(linkCounter.PAGE_COUNT).increment(1);
	}

	/**
	 * produce an object representation of a wiki page
	 *
	 * @param xml well-formed xml representation of a wiki page
	 */
	private WikiPage parseXml(String xml) {
		WikiPage wikiPage = null;
		try {
			wikiPage = WikiXmlSAXParser.parse(xml);
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return wikiPage;
	}

	public static enum linkCounter {PAGE_COUNT}
}