package multiFileSanitizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class that reads in all the files in a directory, parses them, and combines
 * them into smaller text files. Each file should contain HTML, and running this
 * class's main method will create a new subdirectory. For every 2000 files, two
 * combined files will be created, one containing information for indexing them,
 * the other containing information for displaying them in a web UI.
 * 
 * @author Josh Kessler
 * @version December 2015
 */
public class MultiFileSanitizer {

	static LinkedBlockingQueue<File> allFiles; // for holding all files to read
	LinkedBlockingQueue<HtmlStripper> filesToWrite; // for holding HtmlStripper objects, which will
													// contain data to be written to output files
//	static String rootDirectory = "/home/ubuntu/ebs_mount/sites"; // Path of directory containing all files
																	// to be indexed
	static String rootDirectory = "/Users/joshkessler/Desktop/files"; //for testing locally
	
	static String outputFiles = rootDirectory + "/concatenatedOut"; // Path of directory in which
	// to place output files
	
	static int numDocs;
	DecimalFormat df;

	/**
	 * Constructs a MultiFileSanitizer for the given rootDirectory by reading in
	 * files in directory.
	 * 
	 * @param rootDirectory
	 *            The path of the directory containing files to be parsed.
	 */
	MultiFileSanitizer(String rootDirectory) {
		File file = new File(rootDirectory);

		File[] files = file.listFiles();
		allFiles = new LinkedBlockingQueue<File>();
		allFiles.addAll(Arrays.asList(files));
		for (File f : allFiles) {
			String name = f.getName();
			if (name.contains("DS_Store"))
				allFiles.remove(f);
		}
		numDocs = allFiles.size();
		df = new DecimalFormat();
		df.setMinimumFractionDigits(5);
		df.setMaximumFractionDigits(5);
	}

	public static void main(String[] args) {
		MultiFileSanitizer mfs = new MultiFileSanitizer(rootDirectory);
		File resultsDirectory = new File(outputFiles);
		resultsDirectory.mkdir();

		// removes up to 2000 files at a time from the queue of files to be read
		// and processes them
		while (!allFiles.isEmpty()) {
			LinkedBlockingQueue<File> currentBatch = new LinkedBlockingQueue<File>();
			if (allFiles.size() < 2000) {
				currentBatch = allFiles; // if there are fewer than 2000 files,
											// process them all
			} else {
				for (int i = 0; i < 2000; i++) {
					currentBatch.add(allFiles.remove());
				}
			}
			mfs.parseDocs(currentBatch);
			mfs.combineFiles();
		}
		System.exit(1);
	}

	/**
	 * Concurrently reads in a batch of files, stores their information in
	 * HtmlStripper objects, then writes out condensed files for each batch of
	 * files.
	 * 
	 * @param currentBatch
	 *            A queue of up to 2000 files to be read in.
	 */
	public void parseDocs(LinkedBlockingQueue<File> currentBatch) {
		int threads = currentBatch.size() / 100; // create number of threads
													// relative to number of
													// documents in the batch
													// (goal is roughly 20 docs
													// per thread)
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		filesToWrite = new LinkedBlockingQueue<>();
		for (int i = 0; i < threads; i++) {
			LinkedBlockingQueue<File> threadBatch = new LinkedBlockingQueue<File>();
			if (currentBatch.size() < 100) {
				threadBatch = currentBatch;
			} else {
				for (int j = 0; j < 100; j++) {
					threadBatch.add(allFiles.remove());
				}
			}
			ParserThread parser = new ParserThread(threadBatch);

			// Execute the task
			executor.execute(parser);
		}
		shutDownExecutorService(executor);

		// In case there are remaining tasks due to integer division
		if (!currentBatch.isEmpty()) {
			Thread thread = new Thread(new ParserThread(currentBatch));
			thread.start();
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Concurrently reads in a batch of files, stores their information in
	 * HtmlStripper objects, then writes out condensed files for each batch of
	 * files.
	 * 
	 * @param currentBatch
	 *            A queue of up to 2000 files to be read in.
	 */
	private void combineFiles() {
		Writer uiTextWriter = null;
		Writer frequenciesWriter = null;
		String path = null;
		String name = null;
		String newPathPrefix = null;

		// ensure output files have unique names (though they'll be related,
		// randomly, to some input files' names)
		try {
			path = filesToWrite.peek().file.getCanonicalPath();
			name = filesToWrite.peek().file.getName();
			newPathPrefix = path.replace(name, "/concatenatedOut/" + name);
		} catch (IOException e) {
			// TODO
		}

		// create output file for UI-related info
		try {
			File uiOut = new File(newPathPrefix + "UIText.txt");
			if (uiOut.exists()) {
				uiOut.delete();
			}
			uiOut.createNewFile();
			uiTextWriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(uiOut), "utf-8"));
		} catch (IOException e) {
			// TODO
		}

		// create output file for UI-related info
		try {
			File freqOut = new File(newPathPrefix + "Freq.txt");
			if (freqOut.exists()) {
				freqOut.delete();
			}
			freqOut.createNewFile();
			frequenciesWriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(freqOut), "utf-8"));
		} catch (IOException e) {
			// TODO
		}

		// Create one thread for writing to each file
		Thread fw = new Thread(new FrequenciesWriterThread(filesToWrite,
				frequenciesWriter));
		Thread ui = new Thread(new UiTextWriterThread(filesToWrite,
				uiTextWriter));

		fw.start();
		ui.start();
		try {
			fw.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			ui.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Runnable that holds a list of files from which to read.
	 * 
	 */
	private class ParserThread implements Runnable {
		LinkedBlockingQueue<File> queue;
		File output;

		public ParserThread(LinkedBlockingQueue<File> threadBatch) {
			queue = threadBatch;
			try {
				output = new File(queue.peek().getCanonicalPath() + "combined");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		/**
		 * Pulls files from its queue, reads from files, and manipulates that
		 * data to be stored in HtmlStripper objects.
		 * 
		 */
		@Override
		public void run() {

			while (!queue.isEmpty()) {
				HtmlStripper hs = new HtmlStripper(queue.remove(), df);
				try {
					hs.sanitizeText(hs.readTextFromFile());
					hs.createMap();
					hs.createRelativeFrequency();
					filesToWrite.add(hs); // Add objects to shared queue
											// containing data to be written to
											// files
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
