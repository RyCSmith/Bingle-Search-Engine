package crawler.node.utils;

public class URLInfo {
	private final String hostName;
	private final int portNo;
	private final String filePath;
	private final boolean secure;

	private final String stringRep;

	/**
	 * Constructor called with raw URL as input - parses URL to obtain host name
	 * and file path
	 */
	public URLInfo(String docURL) {
		if (docURL == null || docURL.equals(""))
			throw new IllegalArgumentException("Empty/null doc");
		docURL = docURL.trim();
		if (docURL.startsWith("https://")) {
			secure = true;
		} else if (docURL.startsWith("http://")) {
			secure = false;
		} else {
			throw new IllegalArgumentException("Url missing prefix: " + docURL);
		}
		if (secure && docURL.length() < 9) {
			throw new IllegalArgumentException("Malformed URL prefix");
		}
		if (!secure && docURL.length() < 8) {
			throw new IllegalArgumentException("Malformed URL prefix (http)");
		}
		// Stripping off 'http://'
		docURL = docURL.substring(secure ? 8 : 7);
		/*
		 * If starting with 'www.' , stripping that off too
		 * if(docURL.startsWith("www.")) docURL = docURL.substring(4);
		 */
		int i = 0;
		while (i < docURL.length()) {
			char c = docURL.charAt(i);
			if (c == '/')
				break;
			i++;
		}
		String address = docURL.substring(0, i);
		if (i == docURL.length())
			filePath = "/";
		else
			filePath = docURL.substring(i); // starts with '/'
		if (address.equals("/") || address.equals(""))
			throw new IllegalArgumentException("No hostname (http)");
		if (address.indexOf(':') != -1) {
			String[] comp = address.split(":", 2);
			hostName = comp[0].trim().toLowerCase();
			int in;
			try {
				in = Integer.parseInt(comp[1].trim());
			} catch (NumberFormatException nfe) {
				in = secure ? 443 : 80;
			}
			portNo = in;
		} else {
			hostName = address.toLowerCase();
			portNo = secure ? 443 : 80;
		}
		stringRep = mkString();
	}

	public URLInfo(String hostName, String filePath) {
		if (hostName.endsWith("/")) {
			this.hostName = this.hostName.substring(0, hostName.length() - 1)
					.toLowerCase();
		} else {
			this.hostName = hostName.toLowerCase();
		}
		if (filePath.startsWith("/")) {
			this.filePath = filePath;
		} else {
			this.filePath = "/" + filePath;
		}
		this.portNo = 80;
		this.secure = false;
		stringRep = mkString();
	}

	public URLInfo(String hostName, int portNo, String filePath) {
		if (hostName.endsWith("/")) {
			this.hostName = this.hostName.substring(0, hostName.length() - 1)
					.toLowerCase();
		} else {
			this.hostName = hostName.toLowerCase();
		}
		if (filePath.startsWith("/")) {
			this.filePath = filePath;
		} else {
			this.filePath = "/" + filePath;
		}
		this.portNo = portNo;
		this.secure = false;
		stringRep = mkString();

	}

	public URLInfo(String hostName, int portNo, String filePath, boolean secure) {
		if (hostName.endsWith("/")) {
			this.hostName = this.hostName.substring(0, hostName.length() - 1)
					.toLowerCase();
		} else {
			this.hostName = hostName.toLowerCase();
		}
		if (filePath.startsWith("/")) {
			this.filePath = filePath;
		} else {
			this.filePath = "/" + filePath;
		}
		this.portNo = portNo;
		this.secure = secure;
		stringRep = mkString();

	}

	public String getHostName() {
		return hostName;
	}

	// public void setHostName(String s){
	// hostName = s;
	// }

	public int getPortNo() {
		return portNo;
	}

	// public void setPortNo(int p){
	// portNo = p;
	// }

	public String getFilePath() {
		return filePath;
	}

	// public void setFilePath(String fp){
	// filePath = fp;
	// }

	public boolean isSecure() {
		return secure;
	}

	public static URLInfo appendFP(URLInfo u, String newFP) {
		String s = u.filePath;
		int idx = s.lastIndexOf("/");
		if (idx > -1) {
			s = s.substring(0, idx);
		}
		if (!newFP.startsWith("/")) {
			return new URLInfo(u.hostName, u.portNo, s + "/" + newFP, u.secure);
		} else {
			return new URLInfo(u.hostName, u.portNo, s + newFP, u.secure);
		}
	}

	public static URLInfo withFP(URLInfo u, String newFP) {
		return new URLInfo(u.hostName, u.portNo, newFP, u.secure);
	}

	@Override
	public int hashCode() {
		return stringRep.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof URLInfo) {
			return ((URLInfo) o).stringRep.equals(stringRep);
		}
		return false;
	}

	@Override
	public String toString() {
		return stringRep;
	}

	public boolean domainEquals(URLInfo u) {
		if (u == null) {
			return false;
		}
		return u.getHostName().equals(hostName) && u.isSecure() == secure
				&& u.getPortNo() == portNo;
	}

	private String mkString() {
		StringBuilder s = new StringBuilder();
		if (secure) {
			s.append("https://");
		} else {
			s.append("http://");
		}
		s.append(hostName);
		if ((secure && portNo != 443) || (!secure && portNo != 80)) {
			s.append(":");
			s.append(portNo);
		}
		s.append(filePath);
		return s.toString();
	}

}
