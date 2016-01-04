package crawler.node.core;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;

import crawler.node.utils.HttpClient.HttpResponse;
import crawler.node.utils.URLInfo;

public class ExtractorThread extends Thread {

	private CrawlManager manager;
	private ConcurrentLinkedQueue<HttpResponse> extractorQ;
	private static NullOutputStream n1 = new NullOutputStream();
	private static PrintWriter n = new PrintWriter(new NullOutputStream());
	private static final Logger logger = Logger
			.getLogger(ExtractorThread.class);

	private static class NullOutputStream extends OutputStream {
		@Override
		public void write(int x) {

		}

		@Override
		public void write(byte[] b) {

		}

		@Override
		public void write(byte[] b, int a, int c) {

		}

	}

	public ExtractorThread() {
		manager = CrawlManager.getManager();
		extractorQ = manager.extractorQ;
	}

	private static LinkedList<String> extract(HttpResponse hr) {
		LinkedList<String> urlQ = new LinkedList<>();
		Tidy t = new Tidy();
		t.setErrout(n);
		t.setForceOutput(true);
		try {
			Document d = t.parseDOM(new ByteArrayInputStream(hr.data), n1);
			NodeList n = d.getElementsByTagName("a");
			for (int i = 0; i < n.getLength(); i++) {
				NamedNodeMap attrs = n.item(i).getAttributes();
				Node href = attrs.getNamedItem("href");
				if (href == null) {
					continue;
				}
				Node lang = attrs.getNamedItem("lang");
				if (lang != null && !lang.equals("en")) {
					continue;
				}

				String url = href.getNodeValue();
				String urllower = url.toLowerCase();

				if (url.startsWith("#") || urllower.startsWith("javascript:")
						|| urllower.startsWith("mailto:")) {
					continue;
				}

				logger.debug("URL IS: " + url);
				if (url.startsWith("http://") || url.startsWith("https://")) {
					urlQ.addLast(url.substring(0, 7)
							+ URLEncoder.encode(url.substring(7), "UTF-8"));
				} else if (url.startsWith("//")) {
					urlQ.addLast((hr.u.isSecure() ? "https:" : "http:")
							+ URLEncoder.encode(url, "UTF-8"));
				} else if (url.startsWith("/")) {
					urlQ.addLast(URLInfo.withFP(hr.u,
							URLEncoder.encode(url, "UTF-8")).toString());
				} else {
					urlQ.addLast(URLInfo.appendFP(hr.u,
							URLEncoder.encode(url, "UTF-8")).toString());
				}
			}
		} catch (Exception e) {
			logger.debug(e);
		}
		return urlQ;
	}

	@Override
	public void run() {
		logger.debug("Extractor thread starting");
		while (true) {
			try {
				HttpResponse hr = extractorQ.poll();
				if (hr == null) {
					try {
						Thread.sleep(2 * 1000);
					} catch (InterruptedException e) {
					}
					continue;
				}
				LinkedList<String> links = extract(hr);
				manager.sendTodos(links);
			} catch (Exception e) {
				continue;
			}
		}
	}

}
