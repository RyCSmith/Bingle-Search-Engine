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

/**
 * Utility to interact with PageRank database hosted on AWS by Max Tromhauer.
 */
@Service
public class PageRank {
	
	final static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider("max"));
	DynamoDB dynamoDB = new DynamoDB(client);
	
	/**
	 * Fetches a DynamoDB Item containing PageRank info for a given docID.
	 * @param docID - document ID
	 * @return item - DynamoDB Item containing PageRank info.
	 */
	@Cacheable("prCache")
	public Item getPageRank(String docID) {
		try {
			long id = Long.parseLong(docID);
			Table table = dynamoDB.getTable("PageRank");
			Item item = table.getItem("fingerprint", id);
			return item;
		} catch (Exception e) {
			return null;
		}
	}
	
}
