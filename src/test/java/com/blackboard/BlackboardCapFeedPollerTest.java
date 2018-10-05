package com.blackboard;


import com.google.publicalerts.cap.Alert;
import java.net.MalformedURLException;
import java.net.URL;
import static org.junit.Assert.*;
import org.junit.Test;


/**
 *
 * @author Matt McHenry
 */
public class BlackboardCapFeedPollerTest {

    @Test
    public void testGetDispatchSource() throws MalformedURLException {
        BlackboardCapFeedPoller poller = new BlackboardCapFeedPoller(221729, "MetisTest");
        assertEquals("MetisTest", poller.getName());

        poller = new BlackboardCapFeedPoller(
                new URL("http://cap.blackboardconnect.com/221729/MetisTest/feed.xml"));
        assertEquals("221729/MetisTest", poller.getName());
    }

//    @Test
//    public void testIntegration() throws Exception {
//        BlackboardCapFeedPoller poller = new BlackboardCapFeedPoller(221729, "MetisTest");
//        poller.setHandler(new BlackboardCapFeedPoller.Handler() {
//            @Override
//            public void handleEntryAdded(BlackboardCapFeedPoller feed, String url, Alert cap) {
//                System.out.println("added CAP from " + url + ":\n" + cap);
//            }
//            @Override
//            public void handleEntryRemoved(BlackboardCapFeedPoller feed, String url) {
//                System.out.println("removed CAP from " + url);
//            }
//        });
//        poller.run();
//    }

}
