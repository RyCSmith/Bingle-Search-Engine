package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private final static Text wordInfo = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] allWords = value.toString().split("\n");

		for (String wordLine : allWords) {
			String[] parts = wordLine.split(":", 2);
			String term = parts[0];
			String termInfo = parts[1];
			word.set(term);
			wordInfo.set(termInfo);
			context.write(word, wordInfo);
		}

	}
}
