
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

/**
 * Class that reads in MapReduce output files in a directory, parses them, and uploads
 * records based on the file contents to DynamoDB.
 * 
 * @author Josh Kessler
 * @version December 2015
 */
public class EmrOutputReader {
	
	static String inputDir = "/home/ubuntu/data/EMROut";
//	static String inputDir = "/Users/joshkessler/Desktop/testdynamo";
	static String errorOut = "/home/ubuntu/data/error";
	static File errorOutputFile;
	static String filename;
	static final int maxLength = 4100;
	static LinkedBlockingQueue<String> queue;
	static boolean allRead;
    static AmazonDynamoDBClient client;
    static List<String> failedSaves;
    static File errorDir;

    /**
     * Set up credentials for DynamoDB
     */
    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/Users/joshkessler/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/joshkessler/.aws/credentials), and is in valid format.",
                    e);
        }
        client = new AmazonDynamoDBClient(credentials);
        Region us1east = Region.getRegion(Regions.US_EAST_1);
        client.setRegion(us1east);
    }

    public static void main(String[] args) throws Exception {
    	System.out.println("starting at: " + System.currentTimeMillis());
        init();

		File input = new File(inputDir);
		errorDir = new File(errorOut);
		errorDir.mkdir();
		failedSaves = new LinkedList<>();
		client = new AmazonDynamoDBClient();
		File[] allFiles = input.listFiles();
		
		//there are multiple output files from mapreduce. read through each one individually
		for (int i = 0; i < allFiles.length; i++) {
			FileReaderThread frt = new FileReaderThread(allFiles[i]); //one thread reads from file and adds each line to shared queue
			frt.start();
			
			ExecutorService executor = Executors.newFixedThreadPool(10);

			for (int j = 0; j < 10; j++) {
				DynamoWriterThread dwt = new DynamoWriterThread(queue); //these threads pull from line queue and try to send batch requests to
																		//DynamoDB

				// Execute the task
				executor.execute(dwt);
			}
			shutDownExecutorService(executor);

			
			try {
				frt.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//if any batch writes failed, write them to an output file to track them
			if (failedSaves.size() != 0){
		        errorOutputFile = new File(errorOut + "/" + i + ".txt");
		        errorOutputFile.createNewFile();
		        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(errorOutputFile), "utf-8"));
		        synchronized(failedSaves){
			        for (String failure: failedSaves){
			        	writer.write(failure);
			        	writer.write("\n");
			        }
		        }
		        writer.close();
		        
			}
		}
		System.out.println("ending at: " + System.currentTimeMillis());
    }

    /**
     * Thread  reads from a file line by line and feeds strings to a queue of worker threads
     */
	static class FileReaderThread extends Thread {
		File file;

		public FileReaderThread(File file) {
			this.file = file;
			allRead = false;
		}

		@Override
		public void run() {
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				queue = new LinkedBlockingQueue<>();
				String line;
				int i = 0;
				while ((line = reader.readLine()) != null) {
					if (line.length() > maxLength) {
						continue;
					} else {
						queue.add(line);
					}
					
					while(queue.size() > 1000){ //to avoid running out of memory and give writer threads time to catch up
						
					}
					
				}
				System.out.println("finished");
				allRead = true;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
    /**
     * Thread that pulls strings from a shared queue, generates DynamoDB Items, and writes batches to a Table.
     */
	static class DynamoWriterThread extends Thread {
		Queue<String> myEntries;
		DynamoDBMapper mapper;
		LinkedBlockingQueue<String> workPool;

		public DynamoWriterThread(LinkedBlockingQueue<String> workPool) {
			mapper = new DynamoDBMapper(client);
			this.workPool = workPool;
			myEntries = new LinkedBlockingQueue<>();
		}

		@Override
		public void run() {
			while (!allRead) {
				if (workPool != null && !workPool.isEmpty()) {
					String task = workPool.poll();
					if (task != null){
						myEntries.add(task);
						if (myEntries.size() == 25) { //write batches of 25 (max size for DDB)
							writeToDB();	
						}
					}
				}
			}
			if (!myEntries.isEmpty()) {
				writeToDB(); //in case there aren't enough lines for a full batch
			}

		}

	    /**
	     * Writes a batch of of 25 (or all remaining items, if there are fewer than 25) to DynamoDB.
	     */
		public void writeToDB() {
			ArrayList<InvertedIndexEntry> batch = new ArrayList<>();
			for (String text : myEntries) {
				batch.add(new InvertedIndexEntry(text));
			}
			java.util.List<DynamoDBMapper.FailedBatch> result = null;
			try{
				
			//Dynamo returns a set of failed results
			result = mapper.batchSave(batch);
			} catch(Exception e){ //Amazon is throwing an interrupted exception here
				//TODO
			}
			
			//save failed results to a file for retrying later
			if (result!= null && result.size() != 0){
				addFailuresToList(result);
			}

			myEntries = new LinkedBlockingQueue<String>();

			}
		}
		
    /**
     * Adds data from failed batch submissions to a shared queue in readable format
     */
		public static void addFailuresToList(java.util.List<DynamoDBMapper.FailedBatch> failures){
			for (DynamoDBMapper.FailedBatch batch : failures){
				Collection<List<WriteRequest>> things = batch.getUnprocessedItems().values();
				for (List<WriteRequest> wqList : things){
					for (WriteRequest wq : wqList){
						synchronized (failedSaves){
							failedSaves.add(wq.toString());
						}
					}
				}
			}
		}
		
		/**
		 * Shuts down executor service.
		 * 
		 * @param executor
		 *            The executor to be shut down.
		 */
		private static void shutDownExecutorService(ExecutorService executor) {
			executor.shutdown();
			try {
				if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				executor.shutdownNow();

				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
		}

}