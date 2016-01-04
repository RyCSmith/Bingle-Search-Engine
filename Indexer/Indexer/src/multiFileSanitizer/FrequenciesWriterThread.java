package multiFileSanitizer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Thread class that writes text extracted from multiple input files to a single output file.
 * Writes information about term frequencies and locations within a document.
 * @author Josh Kessler
 * @version December 2015
 */
public class FrequenciesWriterThread implements Runnable {
	ArrayList<HtmlStripper> files;
	Writer frequenciesWriter;

	public FrequenciesWriterThread(LinkedBlockingQueue<HtmlStripper> filesToWrite, Writer frequenciesWriter) {
		files = new ArrayList<>();
		files.addAll(filesToWrite);
		this.frequenciesWriter = frequenciesWriter;

	}

	@Override
	public void run() {
		int i = 4;
		if (!(frequenciesWriter == null)) {
			for (HtmlStripper hs : files) {
				try {
					String write = hs
							.writeLocationsAndFrequenciesString();
					frequenciesWriter.write(write);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				frequenciesWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
