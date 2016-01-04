package crawler.node.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.util.ByteArrayBuffer;
import org.apache.log4j.Logger;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;

public class HttpClient {
	public static final int TIMEOUT = 6000; // ms
	public static final int maxSize = 5 * 1000000;
	public static final int MAX_REDIRECT = 3;
	public static final byte[] DEFAULT_HEADERS = ("User-Agent: cis455crawler\r\n"
			+ "Accept-Language: en-us\r\n"
			+ "Accept-Encoding: gzip, deflate\r\n"
			+ "Connection: close\r\n\r\n").getBytes();

	private static final Logger logger = Logger.getLogger(HttpClient.class);

	public static class HttpClientException extends RuntimeException {
		private static final long serialVersionUID = -6650265225237968850L;

		public HttpClientException(String msg) {
			super(msg);
		}
	}

	public static class HttpResponse {
		public int status;
		public Map<String, List<String>> headers;
		public byte[] data;
		public int redirect;
		public URLInfo u;
		public long contentLength;
		public String contentType;
	}

	public static HttpResponse headRequest(URLInfo url, String date) {
		int attempts = 0;
		URLInfo u = url;
		HttpResponse hr = runRequest(url, "HEAD", date);
		while (hr.status == 301 && ++attempts < MAX_REDIRECT) {
			try {
				String loc = hr.headers.get("Location").get(0);
				if (loc.startsWith("http://") || loc.startsWith("https://")) {
					u = new URLInfo(loc);
				} else {
					u = URLInfo.withFP(u, loc);
				}
				logger.debug("Redirecting to " + u.toString());
				hr = runRequest(u, "HEAD", date);
				hr.redirect = attempts;
			} catch (NullPointerException npe) {
				throw new HttpClientException("No location given for 301");
			}
		}
		return hr;
	}

	public static HttpResponse getRequest(URLInfo url) {
		int attempts = 0;
		URLInfo u = url;
		HttpResponse hr = runRequest(url, "GET", null);
		while (hr.status == 301 && ++attempts < MAX_REDIRECT) {
			try {
				String loc = hr.headers.get("Location").get(0);
				if (loc.startsWith("http://") || loc.startsWith("https://")) {
					u = new URLInfo(loc);
				} else {
					u = URLInfo.withFP(u, loc);
				}
				logger.debug("Redirecting to " + u.toString());
				hr = runRequest(u, "GET", null);
				hr.redirect = attempts;
			} catch (NullPointerException npe) {
				throw new HttpClientException("No location given for 301");
			}
		}
		return hr;
	}

	private static HttpResponse runRequest(URLInfo u, String requestType,
			String date) {
		OutputStream o;
		InputStream i;
		if (u.isSecure() || u.getFilePath().equals(u.getFilePath()) ) {
			URL x;
			try {
				x = new URL(u.toString());
			} catch (MalformedURLException e1) {
				throw new HttpClientException("Malformed URL");
			}

			try {
				HttpsURLConnection.setFollowRedirects(false);
				HttpURLConnection.setFollowRedirects(false);
				HttpURLConnection ux;
				
				if (u.isSecure()) {
					ux = (HttpsURLConnection) x.openConnection();
				} else {
					ux = (HttpURLConnection) x.openConnection();
				}
				ux.setRequestMethod(requestType);

				if (date != null) {
					ux.setIfModifiedSince(ServerUtils.dateFromString(date)
							.getTime());
				}
				ux.setRequestProperty("User-Agent", "cis455crawler");
				ux.setRequestProperty("Accept-Encoding", "gzip, deflate");


				ux.setReadTimeout(TIMEOUT);

				i = ux.getErrorStream();

				HttpResponse hr = new HttpResponse();
				hr.status = ux.getResponseCode();
				hr.contentLength = ux.getContentLengthLong();

				hr.contentType = ux.getContentType();

				hr.headers = ux.getHeaderFields();
				hr.u = u;

				if (!requestType.equals("GET")) {
					return hr;
				}
				
				if (i == null) {
					String enc = ux.getContentEncoding();
					if (enc == null) {
						if (hr.headers.containsKey("Content-Encoding")) {
							enc = hr.headers.get("Content-Encoding").get(0);
						}
					}
					if (enc == null || !enc.equals("gzip")) {
						i = ux.getInputStream();
					} else {
						i = new GZIPInputStream(ux.getInputStream());

					}
				}

				
				int count = ux.getContentLength();
				int totalCount = 0;
				byte[] buf = new byte[1024];
				
				ByteArrayOutputStream bs = new ByteArrayOutputStream();
				
				while ((count = i.read(buf, 0, buf.length)) > 0 && totalCount < maxSize ) {
					bs.write(buf);
					totalCount += count;
				}
			
				if (totalCount < maxSize) {
					hr.data = bs.toByteArray();
				}
				return hr;
			} catch (Exception e) {
				logger.debug("HTTPS EXCEPTION!", e);
				throw new HttpClientException("Unable to connect");
			}

		} else {
			InetAddress a;
			try {
				a = InetAddress.getByName(u.getHostName());
			} catch (UnknownHostException e1) {
				throw new HttpClientException("Unknown hostname");
			}
			InetSocketAddress address = new InetSocketAddress(a, u.getPortNo());

			Socket s = new Socket();
			try {
				s.bind(null);
			} catch (IOException e1) {
				try {
					s.close();
				} catch (IOException e) {
				}
				throw new HttpClientException("Could not bind socket to port");
			}

			try {
				s.setSoTimeout(TIMEOUT);
				s.connect(address);
				o = s.getOutputStream();
				i = s.getInputStream();
			} catch (IOException e) {
				try {
					s.close();
				} catch (IOException be) {
				}
				throw new HttpClientException(
						"Could not connect to specified address");
			}
			try {
				writeRequest(u, o, requestType, date);
			} catch (IOException e) {
				try {
					s.close();
				} catch (IOException ex) {
				}
				throw new HttpClientException("Error writing request to socket");
			}
			try {
				HttpResponse hrr = readResponse(i, requestType.equals("GET"),
						maxSize);
				hrr.u = u;
				return hrr;
			} catch (IOException e) {
				logger.debug("IOException plain Http response", e);
				throw new HttpClientException("Error reading response");
			} finally {
				try {
					s.close();
				} catch (IOException e) {
				}
			}
		}

	}

	private static void writeRequest(URLInfo u, OutputStream o,
			String requestType, String modifiedSince) throws IOException {
		byte[] getReq = (requestType + " " + u.getFilePath() + " HTTP/1.1\r\n" + "Host: www.cnn.com\r\n")
				.getBytes();
		o.write(getReq);
		if (modifiedSince != null) {
			o.write(("If-Modified-Since: " + modifiedSince + "\r\n").getBytes());
		}
		o.write(DEFAULT_HEADERS);
		o.flush();
	}

	private static HttpResponse readResponse(InputStream s, boolean data,
			int maxSize) throws IOException {

		byte[] response = IOUtils.toByteArray(s);

		BufferedReader i = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(response)));

		String line1 = i.readLine();

		if (line1 == null || line1.isEmpty()) {
			throw new HttpClientException("Empty response");
		}

		String[] cmds = line1.trim().split("[ \t]");
		if (cmds.length <= 1) {
			throw new HttpClientException("Bad response: " + line1);
		}

		HttpResponse hr = new HttpResponse();

		hr.status = 0;
		for (int j = 1; j < cmds.length; j++) {
			try {
				hr.status = Integer.parseInt(cmds[j]);
				break;
			} catch (NumberFormatException e) {
			}
		}

		if (hr.status == 0) {
			throw new HttpClientException("No status in response: " + line1);
		}

		hr.headers = parseHeaders(i);

		i.close();
		
		if (!data || hr.status != 200) {
			return hr;
		}

		int dataStartIndex = -1;
		for (int ix = 0; ix < response.length - 3; ix++) {
			if (response[ix] == 0x0d && response[ix + 1] == 0x0a
					&& response[ix + 2] == 0x0d && response[ix + 3] == 0x0a) {
				dataStartIndex = ix+4;
				break;
			}
		}
		
		if (dataStartIndex < 0) {
			return hr;
		}
		
		if (response.length - dataStartIndex > maxSize) {
			return hr;
		}

		if (hr.headers.containsKey("Content-Encoding")) {
			List<String> sz = hr.headers.get("Content-Encoding");
			String sy = sz.get(0);
			if (sy != null && sy.equals("gzip")) {
				try {
					byte[] newStr = IOUtils.toByteArray(new GZIPInputStream(
							new ByteArrayInputStream(response, dataStartIndex, maxSize)));
					hr.data = newStr;
				} catch (IOException e) {
					logger.debug("GZIP!", e);
					logger.debug(dataStartIndex);
//					logger.debug()
					logger.debug(Hex.encodeHexString(IOUtils.toString(new ByteArrayInputStream(response, 0 , maxSize)).getBytes()));
					return hr;
				}

			}
		}
		
		if (hr.data == null) {
			hr.data = IOUtils.toByteArray(new ByteArrayInputStream(response,dataStartIndex,maxSize));
		}

		return hr;
	}

	private static Map<String, List<String>> parseHeaders(BufferedReader i) {
		TreeMap<String, List<String>> headers = new TreeMap<>();
		List<String> l = null;
		while (true) {
			String line = null;
			try {
				line = i.readLine();
			} catch (SocketTimeoutException e) {
				throw new RuntimeException("timeout");
			} catch (IOException e) {
				throw new RuntimeException("IO?Exception");
			}
			if (line == null || line.isEmpty()) {
				break;
			}
			if (line.contains(":")) {
				int idx = line.indexOf(':');
				String headerKey = line.substring(0, idx);
				l = headers.get(headerKey);
				if (l == null) {
					l = new LinkedList<String>();
					headers.put(headerKey, l);
				}
				String headerVal = line.substring(idx + 1).trim();
				l.add(l.size(), headerVal);
			} else {
				if (l == null || l.isEmpty()) {
					break;
				}
				String headerVal = l.remove(l.size() - 1);
				headerVal += line.trim();
				l.add(l.size(), headerVal);
			}
		}
		return headers;
	}

}
