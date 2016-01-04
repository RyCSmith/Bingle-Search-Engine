package multiFileSanitizer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Thread class that writes text extracted from multiple input files to a single output file.
 * Writes information about a document for use by a search engine UI.
 * @author Josh Kessler
 * @version December 2015
 */
public class UiTextWriterThread implements Runnable {

	ArrayList<HtmlStripper> files;
	Writer uiTextWriter;

	UiTextWriterThread(LinkedBlockingQueue<HtmlStripper> filesToWrite, Writer uiTextWriter) {
		files = new ArrayList<>();
		files.addAll(filesToWrite);
		this.uiTextWriter = uiTextWriter;
	}

	@Override
	public void run() {
		if (!(uiTextWriter == null)) {

			for (HtmlStripper hs : files) {
				try {
					uiTextWriter.write(hs.writeDataForUiToFile());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				uiTextWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
