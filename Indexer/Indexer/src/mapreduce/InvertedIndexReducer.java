package mapreduce;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	int numDocs = 0;
	static Pattern findUrl = Pattern.compile("\\:.*\\:");
	//each line contains "word:docID:TFScore:location1, location2
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws java.io.IOException, InterruptedException {
		
		DecimalFormat df = new DecimalFormat();
		df.setMinimumFractionDigits(5);
		df.setMaximumFractionDigits(5);
		
		//get all values from iterable and cache them
		ArrayList<DocInfo> cache = new ArrayList<DocInfo>();
        for (Text t : values) {
            Text docInfo = new Text();
            docInfo.set(t.toString());
            cache.add(new DocInfo(docInfo, df));
        }
        
        int docOccurrences = cache.size();
        double idf = Math.log(numDocs / (double) docOccurrences);
        
        for (DocInfo doc : cache){
        	doc.changeTfToTfIdfProduct(idf);
        }
        
        DocInfoComparator comp = new DocInfoComparator();
        cache.sort(comp);
        
        for (DocInfo doc : cache){
        	doc.changeTfToTfIdfProduct(idf);
        }
		
        StringBuffer sb = new StringBuffer();
        for (DocInfo doc : cache){
			if(sb.length() != 0) {
				sb.append("|"); //output will be word:docID1:TFScore1:location1, location2|docID1:TFScore1:location1, location2
			}
			sb.append(doc.toString());
		}
		
		Text documentList = new Text();
		documentList.set(sb.toString());
		context.write(key, documentList);
	}
	
}