package crawler.node.io;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

public class DynamoInterface {

	private static final AmazonDynamoDBClient client = new AmazonDynamoDBClient(
			new ProfileCredentialsProvider());

	private static final DynamoDBMapper mapper = new DynamoDBMapper(client);

	@DynamoDBTable(tableName = "siterecord")
	public static class SiteInfo {
		private String url;
		private long crawlDate;
		private long fingerPrint;

		@DynamoDBHashKey(attributeName = "url")
		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		@DynamoDBAttribute(attributeName = "crawldate")
		public Long getCrawlDate() {
			return crawlDate;
		}

		public void setCrawlDate(Long crawlDate) {
			this.crawlDate = crawlDate;
		}

		@DynamoDBAttribute(attributeName = "fingerprint")
		public Long getFingerPrint() {
			return fingerPrint;
		}

		public void setFingerPrint(Long l) {
			fingerPrint = l;
		}

		public void save() {
			while (true) {
				try {
					mapper.save(this);
					break;
				} catch (ProvisionedThroughputExceededException e) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					}
				}
			}

		}
	}

	public static SiteInfo getSiteInfo(String url) {
		while (true) {
			try {
				SiteInfo ste = mapper.load(SiteInfo.class, url);
				return ste;
			} catch (ProvisionedThroughputExceededException e) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
			}
		}
	}

	public static FingerPrintRecord getFPRecord(long fingerprint) {
		while (true) {
			try {
				FingerPrintRecord ste = mapper.load(FingerPrintRecord.class,
						fingerprint);
				return ste;
			} catch (ProvisionedThroughputExceededException e) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
			}
		}
	}

	@DynamoDBTable(tableName = "fingerprints")
	public static class FingerPrintRecord {
		private long fingerprint;
		private String filepath;
		private long lastParsed;

		@DynamoDBHashKey(attributeName = "fingerprint")
		public long getFingerprint() {
			return fingerprint;
		}

		public void setFingerprint(long fingerprint) {
			this.fingerprint = fingerprint;
		}

		@DynamoDBAttribute(attributeName = "filepath")
		public String getFilepath() {
			return filepath;
		}

		public void setFilepath(String filepath) {
			this.filepath = filepath;
		}

		@DynamoDBAttribute(attributeName = "lastparsed")
		public long getLastParsed() {
			return lastParsed;
		}

		public void setLastParsed(long lastParsed) {
			this.lastParsed = lastParsed;
		}

		public void save() {
			while (true) {
				try {
					mapper.save(this);
					break;
				} catch (ProvisionedThroughputExceededException e) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					}
				}
			}
		}

	}

	// public static void saveFingerPrint(String filePath, long fingerprint) {
	// fingerprintTable.putItem(new Item()
	// .withLong("fingerprint", fingerprint).withString("filepath",
	// filePath).withLong("lastparsed", val));
	// }

	// public static String getFilePath(long fingerprint) {
	// Item i = fingerprintTable.getItem("fingerprint", fingerprint);
	// if (i == null){
	// return null;
	// }
	// return (String) i.get("filepath");
	// }

	public static void main(String[] args) {
		// DynamoInterface x = new DynamoInterface();
		// test
		// SiteInfo i = getSiteInfo("http://goo1gle.com");
		// System.out.println(i.getUrl());
		// i.setUrl("http://google.com");
		// mapper.save(i);
		// saveFingerPrint("/joshy", 56L);
		// System.out.println(getFilePath(57L));
		// System.out.println(getFilePath(56L));

	}

}
