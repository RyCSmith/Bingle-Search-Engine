package bingle.search;

/**
 * Basic object to hold data for one Result of a search.
 * Contains URL and Title. May contain page rank, preview and other data.
 *
 */
public class Result {
	private String docID;
	private String url;
	private double pageRank;
	private double tfidf;
	private double weightedValue;
	private String previewName;
	
	/**
	 * TFIDF constructor.
	 * @param url
	 * @param pageRank
	 * @param tfidf
	 */
	public Result(String docID, double tfidf) {
		this.docID = docID;
		this.tfidf = tfidf;
	}
	
	/**
	 * Full args constructor.
	 * @param url
	 * @param pageRank
	 * @param tfidf
	 */
	public Result(String docID, String url, double pageRank, double tfidf) {
		this.docID = docID;
		this.url = url;
		this.pageRank = pageRank;
		this.tfidf = tfidf;
	}
	
	/*Getters and Setters*/
	
	public String getDocID() {
		return docID;
	}

	public void setDocID(String docID) {
		this.docID = docID;
		
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
		
	}

	public double getTfidf() {
		return tfidf;
	}

	public void setTfidf(double tfidf) {
		this.tfidf = tfidf;
	}
	
	public void incrementTfidf(double increment) {
		tfidf += increment;
	}
	
	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}
	
	public void setWeightedValue(double val) {
		weightedValue = val;
	}
	
	public double getWeightedValue() {
		return weightedValue;
	}
	
	public void setPreviewName(String name) {
		previewName = name;
	}
	
	public String getPreviewName() {
		return previewName;
	}

}
