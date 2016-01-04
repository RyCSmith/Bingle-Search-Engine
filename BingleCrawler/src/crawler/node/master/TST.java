package crawler.node.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class TST {

	public static void main(String[] args) throws IOException {
		
		
		AWSCredentials credentials;
		AmazonSQS sqs;
		
		credentials = new ProfileCredentialsProvider().getCredentials();
		sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(Region.getRegion(Regions.US_EAST_1));
	
		
		BufferedReader r = new BufferedReader(new FileReader(new File("/Users/josh/Desktop/alex500.csv")));
		
		String line = null;
		while ((line = r.readLine()) != null) {
			System.out.println(line);
			sqs.sendMessage(CrawlerMaster.TODOQ, line);
		}
		
		r.close();
		
	}
	
}
