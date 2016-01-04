package crawler.node.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.rabinfingerprint.fingerprint.RabinFingerprintLong;
import org.rabinfingerprint.polynomial.Polynomial;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import crawler.node.io.DynamoInterface;
import crawler.node.io.DynamoInterface.FingerPrintRecord;
import crawler.node.io.DynamoInterface.SiteInfo;
import crawler.node.master.CrawlerMaster;
import crawler.node.utils.HttpClient.HttpResponse;
import crawler.node.utils.URLInfo;

public class CrawlManager {
	public static final Logger logger = Logger.getLogger(CrawlManager.class);
	public static final String DONEQ = CrawlerMaster.DONEQ;
	public static final String TODOQ = CrawlerMaster.TODOQ;
	public static final String MASTER_ANNOUNCE = CrawlerMaster.MASTER_ANNOUNCE;
	public static final String MASTER_RECEIVE = CrawlerMaster.MASTER_RECEIVE;
	private static final Polynomial poly = Polynomial
			.createFromLong(17427110449267851L);

	private int depositCount = 0;
	private long thisCrawlTime;
	private File localStorageDir;
	private static final int crawlThreadNum = 10;
	private static final int extractThreadNum = 10;

	private static CrawlManager manager;

	public static void main(String[] args) {
		getManager();
		while (true) {
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	public static CrawlManager getManager() {
		if (manager == null) {
			manager = new CrawlManager();
			manager.runPieces();
		}
		return manager;
	}

	public File getLocalStorageDir() {
		return localStorageDir;
	}

	public long getCrawlTime() {
		return thisCrawlTime;
	}

	private final AWSCredentials credentials;
	private final AmazonSQS sqs;
	private String myQ;
	private Thread messageReceiver;
	private Thread managementReceiver;
	private Thread[] retrieverThreads;
	private Thread[] extractorThreads;
	private boolean done;

	public final HashMap<String, CrawlDomain> domains;
	public final ConcurrentLinkedQueue<CrawlDomain> domQ;
	public final ConcurrentLinkedQueue<HttpResponse> extractorQ;

	private void runPieces() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cleanupp();
			}
		});
		managementReceiver = new Thread(new ManageReceiver());
		managementReceiver.start();

	}
	
	private void runCrawlerPieces() {
		messageReceiver = new Thread(new MessageReceiver());
		messageReceiver.start();
		logger.debug("Starting crawl threads");
		retrieverThreads = new Thread[crawlThreadNum];
		for (int i = 0; i < crawlThreadNum; i++) {
			retrieverThreads[i] = new Thread(new CrawlThread());
			retrieverThreads[i].start();
		}
		extractorThreads = new Thread[extractThreadNum];
		for (int i = 0; i < extractThreadNum; i++) {
			extractorThreads[i] = new Thread(new ExtractorThread());
			extractorThreads[i].start();
		}
	}

	private CrawlManager() {
		thisCrawlTime = -1;
		done = false;
		domains = new HashMap<>();
		domQ = new ConcurrentLinkedQueue<>();
		extractorQ = new ConcurrentLinkedQueue<>();
		credentials = new ProfileCredentialsProvider().getCredentials();
		sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(Region.getRegion(Regions.US_EAST_1));
		myQ = sqs.createQueue(
				new CreateQueueRequest("Req" + new Random().nextInt()))
				.getQueueUrl();
		sqs.sendMessage(new SendMessageRequest(MASTER_RECEIVE, "qnode:" + myQ));
	}

	public void depositDoc(URLInfo u, HttpResponse hr) {
		logger.debug("Deposit #" + depositCount++ + ": " + u.toString());
		
		extractorQ.offer(hr);
		
		RabinFingerprintLong w = new RabinFingerprintLong(poly);
		w.pushBytes(hr.data);
		long fp = w.getFingerprintLong();

		FingerPrintRecord fr = DynamoInterface.getFPRecord(fp);
		if (fr == null) {
			fr = new FingerPrintRecord();
			fr.setFilepath(Long.toString(fp));
			fr.setFingerprint(fp);
			try {
				FileOutputStream f = new FileOutputStream(new File(localStorageDir,
						Long.toString(fp)));
				f.write(hr.data);
				f.close();
			} catch (IOException e) {
				logger.debug("IOException saving doc", e);
				return;
			}
		}
		fr.setLastParsed(thisCrawlTime);
		fr.save();
		
		SiteInfo si = DynamoInterface.getSiteInfo(u.toString());
		if (si == null) {
			si = new SiteInfo();
			si.setUrl(u.toString());
		}
		si.setCrawlDate(manager.getCrawlTime());
		si.setFingerPrint(fp);
		si.save();
	}

	public void sendTodos(List<String> links) {
		sqs.sendMessage(TODOQ, String.join(";", links));
	}

	public void reportDoneLink(String link) {
		sqs.sendMessage(DONEQ, link);
	}

	public void cleanupp() {
		done = true;
		sqs.deleteQueue(myQ);
	}

	// Message Receiving
	public void addNewLinks(List<String> fullPaths) {
		for (String s : fullPaths) {
			URLInfo u;
			try {
				u = new URLInfo(s);
			} catch (IllegalArgumentException e) {
				logger.debug("Issue with url " + s);
				continue;
			}
			URLInfo dom = URLInfo.withFP(u, "/");
			CrawlDomain c = domains.get(dom.toString());
			if (c == null) {
				synchronized (domains) {
					c = domains.get(dom.toString());
					if (c == null) {
						c = new CrawlDomain(dom);
						domains.put(dom.toString(), c);
					}
				}
				domQ.add(c);
			}
			c.enqHead(u.getFilePath());
		}
	}

	private class ManageReceiver implements Runnable {
		private final Logger logger = Logger.getLogger(ManageReceiver.class);

		@Override
		public void run() {
			logger.debug("Management Receiver Running");
			List<Message> msgs;
			while (!done) {
				msgs = sqs.receiveMessage(MASTER_ANNOUNCE).getMessages();
				List<DeleteMessageBatchRequestEntry> del = new LinkedList<>();
				boolean shutdown = false;
				for (Message m : msgs) {
					String s = m.getBody();
					logger.debug("Received message " + s);

					if (s.startsWith("qtime:")) {
						if (thisCrawlTime < 0) {
							thisCrawlTime = Long.parseLong(s.substring(6));
							del.add(new DeleteMessageBatchRequestEntry(m
									.getMessageId(), m.getReceiptHandle()));
							if (localStorageDir != null) {
								runCrawlerPieces();
							}
						}
						continue;
					}

					if (s.startsWith("qpath")) {
						if (localStorageDir == null) {
							localStorageDir = new File(s.substring(6));
							del.add(new DeleteMessageBatchRequestEntry(m
									.getMessageId(), m.getReceiptHandle()));
							if (thisCrawlTime > 0) {
								runCrawlerPieces();
							}
						}
						continue;
					}

					if (s.startsWith("qecmd:")) {
						String cmd = s.substring(6);
						if (cmd.equals("shutdown")) {
							shutdown = true;
							del.add(new DeleteMessageBatchRequestEntry(m
									.getMessageId(), m.getReceiptHandle()));
						}
						break;
					}
				}

				if (!del.isEmpty()) {
					sqs.deleteMessageBatch(MASTER_ANNOUNCE, del);
				}

				if (shutdown) {
					Runtime.getRuntime().exit(0);
				}
			}

		}
	}

	private class MessageReceiver implements Runnable {
		private final Logger logger = Logger.getLogger(MessageReceiver.class);

		@Override
		public void run() {
			logger.debug("Message Receiver Running");
			List<Message> msgs;
			while (!done) {
				msgs = sqs.receiveMessage(myQ).getMessages();
				List<String> fullPaths = new LinkedList<>();
				List<DeleteMessageBatchRequestEntry> del = new LinkedList<>();
				for (Message m : msgs) {
					String s = m.getBody();
					logger.debug("Received message " + s);

					for (String u : s.split(";")) {
						try {
							fullPaths.add(URLDecoder.decode(u, "UTF-8"));
						} catch (UnsupportedEncodingException e) {
						}
					}

					del.add(new DeleteMessageBatchRequestEntry(
							m.getMessageId(), m.getReceiptHandle()));
				}
				addNewLinks(fullPaths);
				if (!del.isEmpty()) {
					sqs.deleteMessageBatch(myQ, del);
				}
			}
		}

	}
}
