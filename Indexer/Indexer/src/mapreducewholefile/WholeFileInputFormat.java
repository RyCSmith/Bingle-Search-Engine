package mapreducewholefile;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable>{

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		return new WholeFileRecordReader((FileSplit) split, job);
	}



}
