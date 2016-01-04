package crawler.node.core;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import crawler.node.utils.HttpClient;
import crawler.node.utils.RobotsTxtInfo;
import crawler.node.utils.URLInfo;

public class CrawlDomain {
	private static final Logger logger = Logger.getLogger(CrawlDomain.class);
	private final URLInfo domain;

	private AtomicBoolean isReady;

	private ArrayList<String> disallowedLinks;
	private ArrayList<String> allowedLinks;
	private ArrayList<String> sitemaps;
	private int crawlDelay;
	private long lastGet;

	private ConcurrentLinkedQueue<String> headQ;
	private ConcurrentLinkedQueue<String> getQ;

	public CrawlDomain(URLInfo domain) {
		this.domain = new URLInfo(domain.getHostName(), domain.getPortNo(),
				domain.getFilePath(), domain.isSecure());
		headQ = new ConcurrentLinkedQueue<>();
		getQ = new ConcurrentLinkedQueue<>();
		isReady = new AtomicBoolean(false);
		getRobots();
	}

	private boolean canDoGet() {
		return isReady.get()
				&& !getQ.isEmpty()
				&& (crawlDelay == 0 || lastGet + crawlDelay < System
						.currentTimeMillis());
	}

	public URLInfo deqGet() {
		if (!canDoGet()) {
			return null;
		}
		lastGet = System.currentTimeMillis();
		return URLInfo.withFP(domain, getQ.poll());
	}

	public URLInfo deqHead() {
		if (!isReady.get() || headQ.isEmpty()) {
			return null;
		}
		return URLInfo.withFP(domain, headQ.poll());
	}

	public void enqHead(String url) {
		if (!headQ.contains(url) && !getQ.contains(url)
				&& disallowedLinks == null || approved(url)) {
			headQ.add(url);
		}
	}

	public void enqHead(ArrayList<String> urls) {
		for (String url : urls) {
			enqHead(url);
		}
	}

	public void enqGet(String url) {
		getQ.add(url);
	}

	public boolean approved(String fp) {
		if (fp == null) {
			return false;
		}
		if (disallowedLinks == null) {
			return true;
		}
		int bestMatch = 0;

		for (String s : disallowedLinks) {
			if (s.length() > 0 && fp.startsWith(s)) {
				bestMatch = Math.max(bestMatch, s.length());
			}
		}

		if (allowedLinks != null) {
			for (String s : allowedLinks) {
				if (s.length() > 0 && fp.startsWith(s)
						&& s.length() > bestMatch) {
					return true;
				}
			}
		}

		return !(bestMatch > 0);
	}

	// Get robots in a different thread
	private void getRobots() {
		new Thread(new Runnable() {
			public void run() {
				URLInfo robotstxt = URLInfo.withFP(domain, "/robots.txt");

				HttpClient.HttpResponse hr;
				try {
					hr = HttpClient.getRequest(robotstxt);
				} catch (HttpClient.HttpClientException e) {
					logger.debug("Robot getting exception:");
					logger.debug(e);
					hr = null;
				}
				if (hr != null && hr.status == 200) {
					logger.debug("Parsing robots");
					RobotsTxtInfo robots = new RobotsTxtInfo();
					Scanner s = new Scanner(new String(hr.data));
					String currUserAgent = null;
					while (s.hasNextLine()) {
						String line = s.nextLine().trim();
						if (line.isEmpty()) {
							continue;
						}
						if (line.startsWith("User-agent:")) {
							currUserAgent = line.substring(11).trim();
							if (!currUserAgent.equals("*")
									&& !currUserAgent.equals("cis455crawler")) {
								currUserAgent = null;
							} else {
								robots.addUserAgent(currUserAgent);
							}
							continue;
						}
						if (line.startsWith("Sitemap:")) {
							robots.addSitemapLink(line.substring(8).trim());
							continue;
						}
						if (currUserAgent == null) {
							continue;
						}
						if (line.startsWith("Disallow:")) {
							robots.addDisallowedLink(currUserAgent, line
									.substring(9).trim());
							continue;
						}
						if (line.startsWith("Allow:")) {
							robots.addAllowedLink(currUserAgent, line
									.substring(6).trim());
							continue;
						}
						if (line.startsWith("Crawl-delay:")) {
							try {
								Integer i = Integer.parseInt(line.substring(12)
										.trim());
								robots.addCrawlDelay(currUserAgent, i);
							} catch (NumberFormatException e) {
							}
						}
					}

					String userAgent = "*";
					if (robots.containsUserAgent("cis455crawler")) {
						userAgent = "cis455crawler";
					}

					allowedLinks = robots.getAllowedLinks(userAgent);
					disallowedLinks = robots.getDisallowedLinks(userAgent);
					sitemaps = robots.getSitemapLinks();
					Integer cd = robots.getCrawlDelay(userAgent);
					if (cd == null) {
						crawlDelay = 0;
					} else {
						crawlDelay = cd * 1000;
						lastGet = System.currentTimeMillis() - cd;
					}
					s.close();
				} else {
					allowedLinks = null;
					disallowedLinks = null;
					crawlDelay = 0;
				}

				isReady.lazySet(true);

				if (sitemaps != null && !sitemaps.isEmpty()) {
					new Thread(new Runnable() {
						public void run() {
							for (String s : sitemaps) {
								headQ.addAll(LinkExtractor
										.siteMapProcess(URLInfo.withFP(domain,
												s)));
							}
						}
					}).run();
				}

				return;

			}
		}).start();
	}

}
