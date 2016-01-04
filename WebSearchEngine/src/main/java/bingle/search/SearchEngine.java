package bingle.search;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.document.Item;

/**
 * Provides operations to interact with search ranking components and return full list of results.
 *
 */
@Service
public class SearchEngine {
	private static final Logger logger = LoggerFactory.getLogger(SearchController.class);

	@Autowired
	private TFIDF tfidf;
	@Autowired
	private PageRank pageRank;
	
	
	/**
	 * Retrieves a set of ranked documents for a given search phrase
	 * using TFIDF and PageRank for ranking.
	 * @param query - full search phrase
	 * @return aggregateTFIDF - List of Results in order of descending relevance
	 */
	@Cacheable("resultsCache")
	public List<Result> fetchAndRankResults(String query) {
		System.out.println ("In execute search. Query: " + query);
		
		//run search and create a list of Result objects containing TFIDF and PageRank
		List<Result> aggregateTFIDF = getAggregateTFIDF(query);
		addPageRank(aggregateTFIDF);
		
		//clean, rank and prepare results
		List<Result> cleanedResults = removeEmptyResults(aggregateTFIDF);
		rank(cleanedResults);
		setPreviewNames(cleanedResults);
		
		logger.info("Completed search and ranking for query \'" 
				+ query + "\'. " + cleanedResults.size() + " results returned.");
		return cleanedResults;
	}
	
	/**
	 * Removes Results for which current info is not available.
	 * @param results - List of Results from search.
	 * @return newRes - Search results with the empty results removed.
	 */
	private List<Result> removeEmptyResults(List<Result> results){
		List<Result> newRes = new ArrayList<Result>();
		for (Result result : results) {
			String url = result.getUrl();
			if (url != null)
				newRes.add(result);
		}
		return newRes;
	}
	
	/**
	 * Calculates a combined weighted score using TF/IDF and PageRank.
	 * Sorts the list in order of highest ranked.
	 * @param results - List of Results from search.
	 */
	private void rank(List<Result> results) {
		for (Result result : results) {
			double tfidf = result.getTfidf();
			double pagerank = result.getPageRank();
			double prWeight = Math.log(Math.abs( (pagerank - 1) / 1.76884 ));
			if (pagerank < 0)
				prWeight -= (prWeight * 2);
			double weightedValue = ((tfidf - 21.6) / 15)+ ((0.1) * prWeight);
			result.setWeightedValue(weightedValue);
		}
		Collections.sort(results, new WeightValComparator());
	}
	
	/**
	 * Creates a preview name by extracting the domain from a URL.
	 * @param results - List of Results from search.
	 */
	private void setPreviewNames(List<Result> results) {
		for (Result result : results) {
			String url = result.getUrl();
			if (url.indexOf("www.") > -1) {
				url = url.substring(url.indexOf("www.") + 4, url.length());
				if (url.indexOf(".") > -1)
					url = url.substring(0,url.indexOf("."));
			}
			else if (url.indexOf("http://") > -1) {
				url = url.substring(url.indexOf("http://") + 7, url.length());
				if (url.indexOf(".") > -1)
					url = url.substring(0,url.indexOf("."));
			}
			else if (url.indexOf("http://.") > -1) {
				url = url.substring(url.indexOf("http://.") + 8, url.length());
				if (url.indexOf(".") > -1)
					url = url.substring(0,url.indexOf("."));
			}
			else if (url.indexOf("https://") > -1) {
				url = url.substring(url.indexOf("https://") + 8, url.length());
				if (url.indexOf(".") > -1)
					url = url.substring(0,url.indexOf("."));
			}
			else if (url.indexOf("https://.") > -1) {
				url = url.substring(url.indexOf("https://.") + 9, url.length());
				if (url.indexOf(".") > -1)
					url = url.substring(0,url.indexOf("."));
			}
			result.setPreviewName(url);
		}
	}
	
	/**
	 * Retrieves TFIDF for each word in a query. 
	 * Creates a list of Results. Aggregates TFIDF for documents that
	 * appear corresponding to more than one word in the query.
	 * @param query - a full search phrase
	 * @return tfidfList - List of Results containing docID and tfidf score
	 */
	public List<Result> getAggregateTFIDF(String query) {
		HashMap<String, Result> resultsMap = new HashMap<String, Result>();
		//clean words according to comply with cleaning in TF/IDF database
		List<String> individualWords = getCleanWords(query);
		
		//get TFIDF results for each word and add TF/IDF scores by document
		for (String word : individualWords) {
			List<String[]> resultsList = tfidf.getURLs(word);
			for (String[] doc : resultsList) {		
				String docID = doc[0];
				double tfidf = Double.parseDouble(doc[1]);
				Result savedResult = resultsMap.get(docID);
				if (savedResult != null) {
					savedResult.incrementTfidf(tfidf);
				}
				else {
					resultsMap.put(docID, new Result(docID, tfidf));
				}
			}
		}
		
		List<Result> tfidfList = new ArrayList<Result>();
		for (String key : resultsMap.keySet()) { 
			tfidfList.add(resultsMap.get(key)); 
		}
		Collections.sort(tfidfList, new tfidfComparator());
		
		logger.info("TFIDF List created for query \'" + query + "\'. Num Results: " + tfidfList.size());
		
		if (tfidfList.size() > 500)
			tfidfList = tfidfList.subList(0, 500);
		return tfidfList;
	}
	
	/**
	 * Takes a query string possibly containing multiple words and cleans the words to 
	 * comply with the format used in the tfidf lookup.
	 * @param query - full search phrase
	 * @return cleanedWords - List<String> containing words in cleaned form
	 */
	private List<String> getCleanWords(String query) {
		String[] stopwords = {"i", "a", "about",
			"an", "are", "as", "at", "be", "by", "com", "for", "from", "how", "in",
			"is", "it", "of", "on", "or", "that", "the", "this", "to", "was", "what",
			"when", "where", "who", "will", "with", "the"};
		String[] words = query.split(" ");
		List<String> cleanedWords = new ArrayList<String>();
		for (String word : words) {
			word = word.trim();
			if (!Arrays.asList(stopwords).contains(word) && word.length() >= 2 && word.length() <= 50) {
				word = word.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
				cleanedWords.add(word);
			}
		}
		return cleanedWords;
	}
	
	/**
	 * Adds PageRank and URL to List of Results. 
	 * Launches one HttpThread per Result object.
	 * @param results - List of Result objects containing, at minimum, docIDs for each Result.
	 */
	public void addPageRank(List<Result> results) {
		HashSet<Thread> threads = new HashSet<Thread>();
		for (Result result : results) {
			HttpThread current = new HttpThread(result);
			current.run();
			threads.add(current);
		}
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Chooses shortest URL from a set of URLs.
	 * (Documents sometimes have more than a single URL linking to them.)
	 * @param urls - Set of URLs mapping to a given document.
	 * @return shortest - String containing the shortest URL from the set.
	 */
	public String chooseURL(HashSet<String> urls) {
		if (urls == null)
			return null;
		String shortest = null;
		int shortestLength = Integer.MAX_VALUE;
		for (String url : urls) {
			if (url.length() < shortestLength) {
				shortestLength = url.length();
				shortest = url;
			}
		}
		return shortest;
	}
	
	/**
	 * Thread to make call to PageRank database and update Result object 
	 * with data obtained.
	 */
	class HttpThread extends Thread {
		
		Result result;
		
		public HttpThread(Result result) {
			this.result = result;
		}
		
		public void run() {
			Item prItem = pageRank.getPageRank(result.getDocID());
			if (prItem != null) {
				result.setPageRank(((BigDecimal) prItem.get("rank")).doubleValue());
				result.setUrl(chooseURL((HashSet<String>) prItem.get("url")));
			}
		}
	}
	
}
