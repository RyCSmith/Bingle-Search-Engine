package crawler.node.master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;

import crawler.node.utils.URLInfo;

public class CrawlerMaster {

	public final Logger logger;
	public static final String DONEQ = "https://sqs.us-east-1.amazonaws.com/837794297508/DoneQ";
	public static final String TODOQ = "https://sqs.us-east-1.amazonaws.com/837794297508/TodoQ";
	public static final String MASTER_ANNOUNCE = "https://sqs.us-east-1.amazonaws.com/837794297508/MasterAnnounce";
	public static final String MASTER_RECEIVE = "https://sqs.us-east-1.amazonaws.com/837794297508/MasterReceive";

	private final AWSCredentials credentials;
	public final AmazonSQS sqs;
	private ArrayList<String> workerQs;
	private boolean done;

	private long crawltime;
	private String mountpath;
	private boolean alternate = false;

	private final ConcurrentLinkedQueue<String> donesQ;
	private final ConcurrentLinkedQueue<String> todos;
	private final Set<String> whitelist;

	private final ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>> allSeen;

	private Thread doneReceiver;
	private Thread todoReceiver;
	private Thread transmitThread;
	private Thread doneThread;
	private Thread stateThread;

	private BigInteger hashUnit;

	private void cleanupp() {
		for (int i = 0; i < workerQs.size(); i++) {
			sqs.sendMessage(MASTER_ANNOUNCE, "qecmd:shutdown");
		}
		done = true;
	}

	public CrawlerMaster(String mountpath) {
		this(mountpath, System.currentTimeMillis(), null);
	}

	public CrawlerMaster(
			String mountpath,
			long crawltime,
			ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>> allSeen) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cleanupp();
			}
		});
		logger = Logger.getLogger(CrawlerMaster.class);
		this.mountpath = mountpath;
		this.crawltime = crawltime;
		credentials = new ProfileCredentialsProvider().getCredentials();
		sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(Region.getRegion(Regions.US_EAST_1));
		done = false;
		if (allSeen == null) {
			this.allSeen = new ConcurrentHashMap<>();
		} else {
			this.allSeen = allSeen;
		}
		workerQs = new ArrayList<String>();
		List<DeleteMessageBatchRequestEntry> del = new LinkedList<>();
		while (true) {
			List<Message> msgs = sqs.receiveMessage(MASTER_RECEIVE)
					.getMessages();
			if (msgs.size() == 0) {
				break;
			}
			for (Message m : msgs) {
				String s = m.getBody();
				logger.debug("Received message " + s);
				if (s.startsWith("qnode:")) {
					String worker = s.substring(6);
					if (!workerQs.contains(worker)) {
						workerQs.add(s.substring(6));
					}
					del.add(new DeleteMessageBatchRequestEntry(
							m.getMessageId(), m.getReceiptHandle()));
				}
			}
			if (!del.isEmpty()) {
				sqs.deleteMessageBatch(MASTER_RECEIVE, del);
			}
		}

		if (allSeen == null) {
			sqs.purgeQueue(new PurgeQueueRequest(DONEQ));
			sqs.purgeQueue(new PurgeQueueRequest(TODOQ));
		}
		sqs.purgeQueue(new PurgeQueueRequest(MASTER_ANNOUNCE));

		for (int i = 0; i < workerQs.size(); i++) {
			sqs.sendMessage(MASTER_ANNOUNCE,
					"qtime:" + Long.toString(crawltime));
			sqs.sendMessage(MASTER_ANNOUNCE, "qpath:" + mountpath);
		}

		hashUnit = new BigInteger("2").pow(160).divide(
				new BigInteger(Integer.toString(workerQs.size())));

		donesQ = new ConcurrentLinkedQueue<>();
		todos = new ConcurrentLinkedQueue<>();
		whitelist = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

		doneReceiver = new Thread(new MessageReceiver(DONEQ, donesQ));
		todoReceiver = new Thread(new MessageReceiver(TODOQ, todos));
		new Thread(new MessageReceiver("https://sqs.us-east-1.amazonaws.com/837794297508/whitelist", whitelist)).start();
		transmitThread = new Thread(new URLTransmitter());
		doneThread = new Thread(new DoneProcessor());
		doneReceiver.start();
		todoReceiver.start();
		transmitThread.start();
		doneThread.start();
		stateThread = new Thread(new StateSaver());
		stateThread.start();
	}

	private int getWorkerByURL(String url) throws NoSuchAlgorithmException {
		byte[] hash = MessageDigest.getInstance("SHA-1").digest(
				new URLInfo(url).getHostName().getBytes());
		return Math.min(new BigInteger(1, hash).divide(hashUnit).intValue(),
				workerQs.size() - 1);
	}

	private class URLTransmitter implements Runnable {
		private final Logger logger = Logger.getLogger(URLTransmitter.class);

		@Override
		public void run() {
			while (!done) {
				try {
					String s = todos.poll();
					if (s == null) {
						try {
							Thread.sleep(2 * 1000);
						} catch (InterruptedException e) {
						}
						continue;
					}
					String decoded = URLDecoder.decode(s, "UTF-8");
					URLInfo full = new URLInfo(decoded);
					String dom = URLInfo.withFP(full, "/").toString();
					if (!whitelist.contains(dom)) {
						continue;
					}
					if (!allSeen.contains(dom)) {
						allSeen.putIfAbsent(dom,
								new ConcurrentHashMap<String, Boolean>());
					}
					ConcurrentHashMap<String, Boolean> sss = allSeen.get(dom);
					if (!sss.contains(full.getFilePath())) {
						sss.put(full.getFilePath(), false);
						sqs.sendMessage(workerQs.get(getWorkerByURL(decoded)),
								s);
					}
				} catch (Exception e) {
					logger.debug("URL Transmitter exception:", e);
				}
			}
		}
	}

	private class DoneProcessor implements Runnable {
		private final Logger logger = Logger.getLogger(DoneProcessor.class);

		@Override
		public void run() {
			while (!done) {
				try {
					String s = donesQ.poll();
					if (s == null) {
						try {
							Thread.sleep(2 * 1000);
						} catch (InterruptedException e) {
						}
						continue;
					}
					URLInfo full = new URLInfo(s);
					String dom = URLInfo.withFP(full, "/").toString();
					if (!allSeen.contains(dom)) {
						allSeen.putIfAbsent(dom,
								new ConcurrentHashMap<String, Boolean>());
					}
					ConcurrentHashMap<String, Boolean> sss = allSeen.get(dom);
					sss.put(full.getFilePath(), true);
				} catch (Exception e) {
					logger.debug("Done processor exception", e);
				}
			}
		}
	}

	private class StateSaver implements Runnable {
		private final Logger logger = Logger.getLogger(StateSaver.class);

		@Override
		public void run() {
			logger.debug("Running StateSaver thread (60s interval)");
			while (!done) {
				try {
					Thread.sleep(60 * 1000);
				} catch (InterruptedException ex) {
				}
				try {
					File fdir = new File(new File(mountpath), "state");
					fdir.mkdir();
					int x = alternate ? 1 : 2;
					ObjectOutputStream f = new ObjectOutputStream(
							new FileOutputStream(new File(fdir, "state-saved-"
									+ Long.toString(crawltime) + "-" + Integer.toString(x))));
					logger.debug("Begin saving object....");
					f.writeObject(allSeen);
					f.close();
					logger.debug("Finished saving object");
					alternate = !alternate;
				} catch (IOException e) {
					logger.debug("state saver IO Exception", e);
				}
			}
		}
	}

	private class MessageReceiver implements Runnable {
		private final Logger logger = Logger.getLogger(MessageReceiver.class);

		private final Collection<String> inQ;
		private final String qUrl;

		public MessageReceiver(String qUrl, Collection<String> inQ) {
			this.qUrl = qUrl;
			this.inQ = inQ;
		}

		@Override
		public void run() {
			logger.debug("Message Receiver Running");
			List<Message> msgs;
			while (!done) {
				try {
					msgs = sqs.receiveMessage(qUrl).getMessages();
					List<String> fullPaths = new LinkedList<>();
					List<DeleteMessageBatchRequestEntry> del = new LinkedList<>();
					for (Message m : msgs) {
						String s = m.getBody();
						logger.debug("Received message " + s);
						for (String u : s.split(";")) {
							fullPaths.add(u);
						}
						del.add(new DeleteMessageBatchRequestEntry(m
								.getMessageId(), m.getReceiptHandle()));
					}
					inQ.addAll(fullPaths);
					if (!del.isEmpty()) {
						sqs.deleteMessageBatch(qUrl, del);
					}
				} catch (Exception e) {
					logger.debug("Message receiver exception", e);
				}
			}
		}

	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("Missing args.");
			return;
		}

		if (args.length == 1) {
			new CrawlerMaster(args[0]);
		}

		else if (args.length > 2 && "--resume".equals(args[1])) {

			File objF = new File(new File(new File(args[0]), "state"),
					"state-saved-" + args[2]);
			try {
				ObjectInputStream ois = new ObjectInputStream(
						new FileInputStream(objF));
				ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>> hmap = (ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>>) ois
						.readObject();
				ois.close();
				CrawlerMaster cm = new CrawlerMaster(args[0],
						Long.parseLong(args[2]), hmap);
				for (Entry<String, ConcurrentHashMap<String, Boolean>> e : hmap
						.entrySet()) {
					URLInfo dom = new URLInfo(e.getKey());
					for (Entry<String, Boolean> e1 : e.getValue().entrySet()) {
						if (!e1.getValue()) {
							cm.todos.add(URLInfo.withFP(dom, e1.getKey())
									.toString());
						}
					}
				}
			} catch (FileNotFoundException e) {
				System.err.println("No old scan found.");
				return;
			} catch (IOException e) {
				e.printStackTrace();
				return;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				return;
			}

		}

		while (true)
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
			}
	}

}
