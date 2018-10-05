package com.blackboard;


import com.google.publicalerts.cap.Alert;
import com.google.publicalerts.cap.CapXmlParser;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;


/**
 *
 * @author Matt McHenry
 */
public class BlackboardCapFeedPoller implements Runnable {

    public interface Handler {
        void handleEntryAdded(BlackboardCapFeedPoller feed, String url, Alert cap);
        void handleEntryRemoved(BlackboardCapFeedPoller feed, String url);
    }

    private Handler handler;

    private final URL url;
    private final String name;

    private boolean initialized = false;
    private long lastModifiedTime = 0;
    private String lastETag;
    private Map<String, SyndEntry> lastEntries;

    private boolean quietErrors = false;


    public BlackboardCapFeedPoller(final URL url) {
        this.url = url;
        if (!"http".equalsIgnoreCase(url.getProtocol())
                && !"https".equalsIgnoreCase(url.getProtocol())) {
            throw new IllegalArgumentException("Blackboard feed must use http url");
        }

        String file = url.getFile();
        int idx = file.lastIndexOf('/');
        if (idx != -1 && idx > 1 && file.charAt(0) == '/') {
            this.name = file.substring(1, idx);
        } else {
            this.name = file;
        }
    }

    public BlackboardCapFeedPoller(final int id, final String name) {
        try {
            this.url = new URL("http", "cap.blackboardconnect.com",
                    "/" + id + "/" + name + "/feed.xml");
            this.name = name;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("unexpected URL exception", ex);
        }
    }

    public void setHandler(final Handler handler) {
        this.handler = handler;
    }


    public String getName() {
        return this.name;
    }

    public URL getURL() {
        return this.url;
    }


    private HttpURLConnection prepareConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) this.url.openConnection();

        // User-Agent ?
        if (this.lastModifiedTime != 0) {
            conn.setIfModifiedSince(this.lastModifiedTime);
        }
        if (this.lastETag != null) {
            conn.setRequestProperty("If-None-Match", this.lastETag);
        }

        return conn;
    }

    private SyndFeed fetchFeed(final HttpURLConnection conn) throws Exception {
        conn.connect();
        try {
            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_NOT_MODIFIED) {
                return null; // feed has not been modified since last read
            } else if (responseCode >= 400 && responseCode < 600) {
                throw new Exception("error loading feed: "
                        + responseCode + " " + conn.getResponseMessage());
            }

            this.lastModifiedTime = conn.getLastModified();
            this.lastETag = conn.getHeaderField("ETag");

            InputStream is = conn.getInputStream();
            try {
                XmlReader reader;
                String contentType = conn.getHeaderField("Content-Type");
                if (contentType != null) {
                    reader = new XmlReader(is, contentType);
                } else {
                    reader = new XmlReader(is);
                }

                SyndFeedInput sfi = new SyndFeedInput();
                return sfi.build(reader);
            } finally {
                is.close();
            }
        } finally {
            conn.disconnect();
        }
    }

    private void processFeed(final SyndFeed feed) {
        Map<String, SyndEntry> curEntries;
        Map<String, SyndEntry> oldEntries = this.lastEntries;

        // add all entries into the current entry set
        List entries = feed.getEntries();
        if (entries == null || entries.isEmpty()) {
            curEntries = null;
        } else {
            curEntries = new HashMap(entries.size());
            for (Object obj : entries) {
                final SyndEntry entry = (SyndEntry) obj;
                final String link = entry.getLink();

                curEntries.put(link, entry);
                if (oldEntries == null || !oldEntries.containsKey(link)) {
                    if (!initialized) {
                        continue; // ignore initial alerts (TODO: should ask user?)
                    }

                    // new link has been added, load CAP, notify handler
                    try {
                        Alert cap = fetchCap(link);

                        if (handler != null) {
                            try {
                                handler.handleEntryAdded(this, link, cap);
                            } catch (Exception ex) {
                                LoggerFactory.getLogger(BlackboardCapFeedPoller.class)
                                        .error("error handling CAP message", ex);
                            }
                        }
                    } catch (SAXParseException ex) {
                        LoggerFactory.getLogger(BlackboardCapFeedPoller.class)
                                .error("error loading CAP url " + link
                                + " [" + ex.getLineNumber() + "," + ex.getColumnNumber() + "]", ex);
                    } catch (Exception ex) {
                        LoggerFactory.getLogger(BlackboardCapFeedPoller.class)
                                .error("error loading CAP url " + link, ex);
                    }
                } // TODO: also periodically reload old entries?
            }
        }

        this.lastEntries = curEntries;

        // process old entries to make sure none removed
        if (oldEntries != null && !oldEntries.isEmpty()) {
            Iterator<Entry<String, SyndEntry>> it
                    = oldEntries.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, SyndEntry> entry = it.next();
                final String link = entry.getKey();
                if (curEntries == null || !curEntries.containsKey(link)) {
                    if (handler != null) {
                        try {
                            handler.handleEntryRemoved(this, link);
                        } catch (Exception ex) {
                                LoggerFactory.getLogger(BlackboardCapFeedPoller.class)
                                        .error("error cancelling CAP message", ex);
                        }
                    }
                }
            }
        }

        this.initialized = true;
    }

    private Alert fetchCap(final String link) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(link).openConnection();
        conn.connect();
        try {
            final int responseCode = conn.getResponseCode();
            if (responseCode >= 400 && responseCode < 600) {
                throw new Exception(responseCode + " " + conn.getResponseMessage());
            }

            InputSource source = new InputSource(conn.getInputStream());
            source.setEncoding(conn.getContentEncoding());
            CapXmlParser parser = new CapXmlParser(false);
            return parser.parseFrom(source);
        } finally {
            conn.disconnect();
        }
    }


    @Override
    public void run() {
        try {
            HttpURLConnection conn = prepareConnection();
            SyndFeed feed = fetchFeed(conn);
            if (feed != null) { // feed null if no changes
                processFeed(feed);
            }
            quietErrors = false;
        } catch (Exception ex) {
            if (!quietErrors) {
                LoggerFactory.getLogger(BlackboardCapFeedPoller.class)
                        .error("error polling blackboard CAP feed", ex);
                quietErrors = true;
            }
        }
    }

}
