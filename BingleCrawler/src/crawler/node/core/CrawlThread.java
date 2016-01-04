package crawler.node.core;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.aspectj.util.FileUtil;

import crawler.node.io.DynamoInterface;
import crawler.node.io.DynamoInterface.FingerPrintRecord;
import crawler.node.io.DynamoInterface.SiteInfo;
import crawler.node.utils.HttpClient;
import crawler.node.utils.HttpClient.HttpResponse;
import crawler.node.utils.ServerUtils;
import crawler.node.utils.URLInfo;

public class CrawlThread extends Thread {

	private static final Logger logger = Logger.getLogger(CrawlThread.class);
	private CrawlManager manager;
	private ConcurrentLinkedQueue<CrawlDomain> domainQ;

	public CrawlThread() {
		manager = CrawlManager.getManager();
		domainQ = manager.domQ;
	}

	private static boolean typeCheck(HttpResponse hr) {
		String type;
		if (hr.contentType != null) {
			type = hr.contentType;
		} else {
			List<String> x = hr.headers.get("Content-Type");
			if (x == null) {
				x = hr.headers.get("Content-type");
			}
			
			if (x == null) {
				x = hr.headers.get("content-type");
			}
			
			if (x == null) {
				return false;
			}
			type = x.get(0);
			if (type == null) {
				return false;
			}
		}

		String[] ctType = type.split(";");
		if (ctType[0].equals("text/html")) {
			return true;
		}
		if (ctType[0].equals("text/xml")) {
			return true;
		}
		if (ctType[0].equals("application/xml")) {
			return true;
		}
		if (ctType[0].endsWith("+xml")) {
			return true;
		}
		return false;
	}

	private static boolean lengthCheck(HttpResponse hr) {

		if (hr.contentLength > 0) {
			return hr.contentLength <= HttpClient.maxSize;
		}

		List<String> x = hr.headers.get("Content-Length");

		if (x == null) {
			logger.debug("No content-length header found");
			return true;
		}

		String len = x.get(0);
		if (len == null) {
			logger.debug("Empty header?");
			return false;
		}
		try {
			Integer i = Integer.parseInt(len);
			if (i <= HttpClient.maxSize) {
				return true;
			}
			logger.debug("size was " + i);
		} catch (NumberFormatException e) {
			logger.debug("Parseint exception");
		}
		return false;
	}

	private void goGetPart(URLInfo todo) {
		logger.debug("GET: " + todo.toString());
		HttpResponse hr;
		try {
			hr = HttpClient.getRequest(todo);
		} catch (HttpClient.HttpClientException c1) {
			logger.debug(c1);
			manager.reportDoneLink(todo.toString());
			return;
		}
		if (hr == null || hr.status != 200) {
			logger.debug("WTH?!: " + todo.toString());
		}
		manager.depositDoc(todo, hr);
		manager.reportDoneLink(todo.toString());
	}

	private void goHeadPart(URLInfo todo, CrawlDomain c) {

		logger.debug("HEAD: " + todo.toString());
		String date = null;
		SiteInfo si = DynamoInterface.getSiteInfo(todo.toString());

		if (si != null) {
			date = ServerUtils.getDate(si.getCrawlDate());
		}
		HttpResponse hr;
		try {
			hr = HttpClient.headRequest(todo, date);
		} catch (HttpClient.HttpClientException c1) {
			logger.debug(c1);
			manager.reportDoneLink(todo.toString());
			return;
		}

		if (hr.status == 304) {
			FingerPrintRecord fp = DynamoInterface.getFPRecord(si
					.getFingerPrint());
			if (fp == null) {
				logger.error("Not modified since, but missing FP record");
				return;
			}
			if (fp.getLastParsed() < manager.getCrawlTime()) {
				try {
					hr.data = FileUtil.readAsByteArray(new File(manager
							.getLocalStorageDir(), fp.getFilepath()));
					manager.extractorQ.offer(hr);
				} catch (IOException e) {
					logger.debug("IO Exception retrieving file", e);
				}
			}
			return;
		}

		if (hr.status != 200) {
			manager.reportDoneLink(todo.toString());
			logger.debug("Status was " + hr.status);
			return;
		}

		// Check content length
		if (!lengthCheck(hr)) {
			manager.reportDoneLink(todo.toString());
			logger.debug("Max size exceeded: " + todo.toString());
			return;
		}

		if (!typeCheck(hr)) {
			logger.debug("type check failed: " + hr.u.toString());
			manager.reportDoneLink(todo.toString());
			return;
		}

		if (hr.redirect > 0) {
			if (todo.domainEquals(hr.u)) {
				if (c.approved(hr.u.getFilePath())) {
					c.enqGet(hr.u.getFilePath());
				}
			} else {
				manager.sendTodos(Collections.singletonList(hr.u.toString()));
			}
		} else {
			c.enqGet(todo.getFilePath());
		}

	}

	@Override
	public void run() {
		logger.debug("Crawl Thread Running");
		CrawlDomain c = null;
		while (true) {
			if (c != null) {
				domainQ.offer(c);
				c = null;
			}
			try {
				c = domainQ.poll();
				if (c == null) {
					try {
						Thread.sleep(2 * 1000);
					} catch (InterruptedException e) {
					}
					continue;
				}
				URLInfo todo;
				if ((todo = c.deqGet()) != null) {
					goGetPart(todo);
				} else if ((todo = c.deqHead()) != null) {
					goHeadPart(todo, c);
				}
			} catch (Exception e) {
				if (e instanceof InterruptedException) {
					break;
				}
				logger.error("CrawlThead Main Catch", e);
			}
		}
	}

}
