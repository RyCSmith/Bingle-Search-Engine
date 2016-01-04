package multiFileSanitizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Class for reading in text downloaded from a crawler from a file, parsing it
 * as a DOM document, and extracting information for a search engine
 * 
 * @author Josh Kessler
 * @version December 2015
 */
public class HtmlStripper {
	File file; // file to be read
	HashMap<String, ArrayList<Integer>> wordLocations; // hashes words to a list
														// of their locations
														// within the document
	HashMap<String, String> wordRelativeFrequency; // hashes words to their TF
													// scores (as a string)
	String docID; // hashed from doc's URL
	String uiDisplayText; // holds first words to appear in doc, to display in
							// UI
	String contents; // non-HTML doc content
	String title; // doc's title
	static final double a = .4; // for calculating normalized TF value
	DecimalFormat df;
	static final String[] STOP_WORDS_LIST = new String[] {"I", "a", "about",
		"an", "are", "as", "at", "be", "by", "com", "for", "from", "how", "in",
		"is", "it", "of", "on", "or", "that", "the", "this", "to", "was", "what",
		"when", "where", "who", "will", "with", "the"};
	static final Set<String> stopwords = new HashSet<String>(Arrays.asList(STOP_WORDS_LIST));

	public HtmlStripper(File file, DecimalFormat df) {
		this.file = file;
		docID = file.getName();
		this.df = df;
	}

	/**
	 * Opens and reads from file, storing contents in this object.
	 * 
	 */
	public String readTextFromFile() throws IOException {
		BufferedReader reader;
		// System.out.println(file);
		reader = new BufferedReader(new FileReader(file));
		String line;
		StringBuffer sb = new StringBuffer();
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}
		reader.close();
		return sb.toString();
	}

	/**
	 * Creates DOM document and extracts title, description, and body contents.
	 * Removes non-alphanumeric characters from contents and extra whitespace
	 * from all fields.
	 * 
	 * @param html
	 *            String representing entire contents of HTML document,
	 *            including HTML markup.
	 */
	public void sanitizeText(String html) {
		Document document = Jsoup.parse(html);
		title = document.title().replaceAll("\\s+", " ");
		contents = document.text().replaceAll("[^A-Za-z0-9\\s]", "")
				.toLowerCase(); // replace all non alphanumeric characters
								// besides hyphen and spaces
		
		
		contents.replaceAll("\\s+", " ");
		
		//get description from document metadata
		try {
			uiDisplayText = document.select("meta[name=description]").first()
					.attr("content").replaceAll("\\s+", " ");
			
			//cut it off at roughly 150 characters (try to find a nearby space delimiter to avoid cutting off words in the middle)
			if (uiDisplayText.length() > 150){
				StringBuffer sb = new StringBuffer();
				if (Character.isWhitespace(uiDisplayText.charAt(150))){
					sb.append(uiDisplayText.substring(0, 150));
					sb.append("...");
				} else{
					sb.append(uiDisplayText.substring(0, 150));
					for (int i = 150; i < uiDisplayText.length(); i++){
						if (Character.isWhitespace(uiDisplayText.charAt(i)) || i > 170){
							sb.append(uiDisplayText.substring(150, i));
							sb.append("...");
							uiDisplayText = sb.toString();
						}
					}

				}
					
			}
		} catch (Exception e) {
			uiDisplayText = "";
		}
	}

	/**
	 * Creates map of word locations.
	 */
	public void createMap() {
		String[] words = contents.split("\\s");
		StringBuilder sb = new StringBuilder();
		wordLocations = new HashMap<String, ArrayList<Integer>>();
		for (int i = 0; i <  words.length; i++) {
			String word = words[i];
			if (word.equals("") || stopwords.contains(word) || word.startsWith("www")) { //remove stopwords and words that were likely websites that got sanitized
				continue;
			}
			ArrayList<Integer> locations;
			if (wordLocations.containsKey(word)) {
				locations = wordLocations.get(word);
			} else {
				locations = new ArrayList<>();
			}
			locations.add(i);
			wordLocations.put(word, locations);
		}
	}

	/**
	 * Creates map of normalized TF values.
	 */
	public void createRelativeFrequency() {
		wordRelativeFrequency = new HashMap<>();
		int max = 0;
		for (ArrayList<Integer> locationsList : wordLocations.values()) {
			if (locationsList.size() > max) {
				max = locationsList.size();
			}
		}
		for (String word : wordLocations.keySet()) {
			double tf = a + ((1 - a) * (wordLocations.get(word).size() / (double) max));
			wordRelativeFrequency.put(word, df.format(tf));
		}
	}

	/**
	 * Returns string based on locations hashmap formatted with delimiters.
	 * String will have form (word:docID:TF:(location)*\n)*
	 * @return The formatted string.
	 */
	public String writeLocationsAndFrequenciesString() {
		StringBuffer sb = new StringBuffer();

		for (String word : wordLocations.keySet()) {
			sb.append(word);
			sb.append(":");
			sb.append(docID);
			sb.append(":");
			sb.append(wordRelativeFrequency.get(word).toString());
			sb.append(":");
			ArrayList<Integer> locationsList = wordLocations.get(word);
			for (int i = 0; i < locationsList.size() - 1; i++) {
				sb.append(locationsList.get(i));
				sb.append(",");
			}
			sb.append(locationsList.get(locationsList.size() - 1));
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * Returns string containing document info for UI.
	 * String will have form docID:title \t UI Display text
	 * @return The formatted string.
	 */
	public String writeDataForUiToFile() {
		StringBuffer sb = new StringBuffer();
		sb.append(docID);
		sb.append(":");
		sb.append(uiDisplayText);
		sb.append("\t");
		sb.append(title);
		sb.append("\n");
		return sb.toString();
	}

}
