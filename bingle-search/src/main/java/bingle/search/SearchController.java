package bingle.search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 * Controller for Bingle Web MVC.
 *
 */
@Controller
public class SearchController {
	
	private static final Logger logger = LoggerFactory.getLogger(SearchController.class);
	
	@Autowired
	SearchEngine searchEngine;
	
	/**
	 * Controller for search query request.
	 * Returns a rendered HTML view.
	 */
	@RequestMapping(value="/", params={"search"})
	public String query(@RequestParam(value="search", required=true) String search, Model model, HttpSession session) {
		logger.info("Received Search request for Query: " + search);
		
		try {
			long start  = System.currentTimeMillis();
			//gets results for query and creates pagination links
			List<Result> resultsList = getResultsForDisplay(search, 0);
			model.addAttribute("resultsList", resultsList);
			List<String[]> paginationLinks = createPaginationLinks(search, 0);
			model.addAttribute("paginationLinks", paginationLinks);
			
			//indicates whether or not demo mode details should be displayed
			if (session.getAttribute("demo") != null) 
				model.addAttribute("demo", true);
			else
				model.addAttribute("demo", false);
			
			//add appropriate header based on results
			if (resultsList.size() > 0)
				model.addAttribute("queryHeader", "Showing results for \"" + search + "\"");
			else
				model.addAttribute("queryHeader", "No results found for \"" + search + "\"");
			long end = System.currentTimeMillis();
			System.out.println("Search Time: " + (end-start));
		} catch (Exception e) {
			model.addAttribute("queryHeader", "No results found for \"" + search + "\"");
			e.printStackTrace();
		}
		
		return "results";
	}
	
	/**
	 * Controller for search query request with start index.
	 * Returns a rendered HTML view.
	 */
	@RequestMapping(value="/", params={"search", "start"})
	public String querySection(@RequestParam(value="search", required=true) String search,
			@RequestParam(value="start", required=true) String start, Model model, HttpSession session) {
		
		logger.info("Received Search request for Query: " + search + " Start: " + start);
		
		try {
			//gets results for query and creates pagination links
			List<Result> resultsList = getResultsForDisplay(search, Integer.parseInt(start));
			model.addAttribute("resultsList", resultsList);
			List<String[]> paginationLinks = createPaginationLinks(search, Integer.parseInt(start));
			model.addAttribute("paginationLinks", paginationLinks);
			
			//indicates whether or not demo mode details should be displayed
			if (session.getAttribute("demo") != null) 
				model.addAttribute("demo", true);
			else
				model.addAttribute("demo", false);
			
			//add appropriate header based on results
			if (resultsList.size() > 0)
				model.addAttribute("queryHeader", "Showing results for \"" + search + "\"");
			else
				model.addAttribute("queryHeader", "No results found for \"" + search + "\"");
		} catch (Exception e) {
			model.addAttribute("queryHeader", "No results found for \"" + search + "\"");
			e.printStackTrace();
		}
		
		return "results";
	}
	
	/**
	 * Controller for home page request.
	 * Returns a rendered HTML view.
	 */
	@RequestMapping(value="/", params={})
	public String home(Model model, HttpSession session) {
		logger.info("Received basic Home request.");
		//add demo object based on whether or not demo is present in session (used for setting demo control button)
		if (session.getAttribute("demo") != null) 
			model.addAttribute("demo", true);
		else
			model.addAttribute("demo", false);
		
		return "home";
	}
	
	/**
	 * Controller to activate/deactivate demo mode for a given user session.
	 * Returns an HTML ResponseEntity acknowledging receipt.
	 */
	@RequestMapping(value="/setdemo", produces = "application/json")
	public ResponseEntity setDemo(Model model, HttpSession session) {
		
		if (session.getAttribute("demo") == null) {
			session.setAttribute("demo", true);
			logger.info("Demo mode activated for session " + session.getId());
		}
		else {
			session.removeAttribute("demo");
			logger.info("Demo mode deactivated for session " + session.getId());
		}
		return new ResponseEntity(HttpStatus.OK);
	}
	
	/**
	 * Controller for random search request. Makes a REST call to outside URL
	 * to obtain a random word, then creates query string and redirects to search controller.
	 */
	@RequestMapping("/random")
	public ModelAndView randomSearch(Model model) {
		BufferedReader in = null;
		String responseString = null;
		try {
			URL obj = new URL("http://randomword.setgetgo.com/get.php");
			HttpURLConnection client = (HttpURLConnection) obj.openConnection();
			client.setConnectTimeout(5000);	
			in = new BufferedReader(new InputStreamReader(client.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			responseString = response.toString().trim();
			logger.info("Retrieved random word: " + responseString + " . Redirecting to search.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			try {
			if (in != null)
				in.close();
			} catch (Exception e){}
		}
		String redirectUrl = "/?search=" + responseString;
		return new ModelAndView("redirect:" + redirectUrl);
	}
	
	/**
	 * Processes search query and returns 10 Results for display (based on start index).
	 * @param query - the search string
	 * @param start - the start index for results
	 * @return List<Results> containing 10 Results for display
	 */
	public List<Result> getResultsForDisplay(String query, int start) {
		List<Result> results = searchEngine.fetchAndRankResults(query);

		if (results.size() < start + 10) {
			return results.subList(start, results.size());
		}
		return results.subList(start, start + 10);
	}
	
	
	/**
	 * This method creates FORWARD/BACKWARD links to be placed on the page based on the search results.
	 * @param query - the search string
	 * @param start - the start index for results
	 * @return navLinks - A list of String[] : [0] url for link, [1] indicates forward or backward
	 */
	public List<String[]> createPaginationLinks(String query, int start) {
		List<String[]> navLinks = new ArrayList<String[]>();
		List<Result> results = searchEngine.fetchAndRankResults(query);
		if (start < 10 && results.size() <= 10) {
			return navLinks;
		}
		if (start < 10) {
			String[] singleForward = {"/?search=" + query + "&start=10", "F"};
			navLinks.add(singleForward);
			return navLinks;
		}
		//reached here start is greater than 10, mod to ensure always multiples of 10
		int rem = start % 10;
		start = start - rem;
		//create Back link
		String[] singleBackward = {"/?search=" + query + "&start=" + (start - 10), "B"};
		navLinks.add(singleBackward);
		
		if (start + 10 < results.size()) {
			String[] singleForward = {"/?search=" + query + "&start=" + (start + 10), "F"};
			navLinks.add(singleForward);
		}
		return navLinks;
	}
	
}
