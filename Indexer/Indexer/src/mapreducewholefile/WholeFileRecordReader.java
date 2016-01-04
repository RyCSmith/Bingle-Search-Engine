package mapreducewholefile;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

public class WholeFileRecordReader implements RecordReader<Text, BytesWritable>{

	private FileSplit filesplit;
	private Configuration conf;
	private boolean processed = false;
	
	public WholeFileRecordReader(FileSplit filesplit, Configuration conf) throws IOException{
		this.filesplit = filesplit;
		this.conf = conf;
	}
	
	@Override
	public void close() throws IOException {
		// do nothing
		
	}

	//TODO check this
	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		return processed ? filesplit.getLength() : 0;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public boolean next(Text key, BytesWritable value) throws IOException {
		if (!processed){
			byte[] contents = new byte[(int) filesplit.getLength()];
			Path file = filesplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
				key.set(file.getName());
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

}
