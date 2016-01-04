package mapreduce;

import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;

public class DocInfo {
	
	Text textContents;
	DecimalFormat df;
	
	public DocInfo (Text textContents, DecimalFormat df){
		this.textContents = textContents;
		this.df = df;
	}
	
	public void changeTfToTfIdfProduct(double idf){
		String stringContents = textContents.toString();
		int startOfTf = stringContents.indexOf(":");
		double tf = Double.parseDouble(stringContents.substring(startOfTf + 1, startOfTf + 8));
		StringBuffer sb = new StringBuffer();
		sb.append(stringContents.substring(0, startOfTf + 1));
		sb.append(df.format(tf*idf));
		sb.append(stringContents.substring(startOfTf + 8, stringContents.length()));
		textContents.set(sb.toString());		
	}
	
	@Override
	public String toString() {
		return textContents.toString();
	}

}
