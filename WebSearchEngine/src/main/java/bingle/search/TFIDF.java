package bingle.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

/**
 * Utility to interact with TFIDF database hosted on AWS by Josh Kessler.
 *
 */
@Service
public class TFIDF {
	
	final AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider("josh"));
	DynamoDB dynamoDB = new DynamoDB(client);
	
	/**
	 * Fetches document IDs and corresponding TFIDF info for a given word.
	 * In case of error or null result from database, return empty ArrayList<String[]>.
	 * @param searchWord - single word from search phrase
	 * @return al - ArrayList of String[] in format {docID, TFIDF rank}
	 */
	@Cacheable("tfidfCache")
	public List<String[]> getURLs(String searchWord) {
		try {
			List<String[]> al = new ArrayList<String[]>();		
			Table table = dynamoDB.getTable("index");
			Item item = table.getItem("word", searchWord);
			String[] docRefs = ((String) item.get("docReferences")).split("\\|");
			for (String current : docRefs) {
				String[] pieces = current.split(":");
				al.add(pieces);
			}
			return al;
		} catch (Exception e) {
			return new ArrayList<String[]>();
		}
	}
}
