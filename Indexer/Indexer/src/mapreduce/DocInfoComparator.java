package mapreduce;

import java.util.Comparator;

public class DocInfoComparator implements Comparator<DocInfo>{

	@Override
	public int compare(DocInfo o1, DocInfo o2) {
		String text1 = o1.textContents.toString();
		String text2 = o2.textContents.toString();
		int colon1 = text1.indexOf(":");
		int colon2 = text2.indexOf(":");
		Double product1 = Double.parseDouble(text1.substring(colon1 + 1, colon1 + 8));
		Double product2 = Double.parseDouble(text2.substring(colon2 + 1, colon2 + 8));
		return product2.compareTo(product1); //to sort in descending order
	}

}
