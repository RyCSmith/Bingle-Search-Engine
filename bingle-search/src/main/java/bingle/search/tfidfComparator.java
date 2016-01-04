package bingle.search;

import java.util.Comparator;

/**
 * Comparator for Result objects.
 * Sorts by TFIDF rank in descending order.
 *
 */
public class tfidfComparator implements Comparator<Result> {
	@Override
	public int compare(Result o1, Result o2) {
		if (o1.getTfidf() < o2.getTfidf())
			return 1;
		if (o1.getTfidf() > o2.getTfidf())
			return -1;
		return 0;
	}
}
