package com.metissecure.plugin.controller;


import com.blackboard.BlackboardCapFeedPoller;
import com.google.publicalerts.cap.Alert;
import com.metissecure.api.alert.DispatchManager;
import com.metissecure.api.alert.ExternalAlertManager;
import com.metissecure.api.alert.MetisCapUtils;
import com.metissecure.model.alert.Dispatch;
import com.metissecure.model.alert.DispatchArguments;
import com.metissecure.model.alert.ExternalAlertSource;
import com.metissecure.spi.alert.ExternalAlertHandler;
import com.metissecure.spi.alert.ExternalAlertSourceType;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;


/**
 *
 * @author Matt McHenry
 */
@Controller("blackboardAlertSource")
@Scope("singleton")
public class BlackboardAlertSourceType implements ExternalAlertSourceType {

    @Autowired DispatchManager dispatchManager;
    @Autowired MetisCapUtils capUtils;

    private final Map<URL, BlackboardAlertHandler> handlers = new HashMap();
    private final Map<String, Dispatch> dispatches = new HashMap();


    @Override
    public String getTypeName() {
        return "Blackboard";
    }


    @Override
    public ExternalAlertHandler createHandler(final ExternalAlertManager mngr,
            final ExternalAlertSource src) throws Exception {
        if (!getTypeName().equalsIgnoreCase(src.getType())) {
            throw new IllegalArgumentException("BlackboardAlertSource"
                    + " cannot handle type " + src.getType());
        } else if (src.getSource() == null || src.getSource().isEmpty()) {
            throw new IllegalArgumentException("BlackboardAlertSource"
                    + " requires non-null source URL");
        }

        URL url = new URL(src.getSource());
        final BlackboardCapFeedPoller poller = new BlackboardCapFeedPoller(url);
        BlackboardAlertHandler handler = new BlackboardAlertHandler(mngr, poller, src);
        synchronized (this) {
            if (this.handlers.get(url) != null) {
                throw new Exception("Blackboard URL " + url + " already registered");
            }
            this.handlers.put(url, handler);
        }

        poller.setHandler(this.FeedHandler);
        handler.threadpool = mngr.getThreadpool();
        handler.future = handler.threadpool
                .scheduleWithFixedDelay(poller, 5, 5, TimeUnit.SECONDS);
        return handler;
    }

    private void removeHandler(final BlackboardAlertHandler handler) {
        handler.shutdown();
        this.handlers.remove(handler.getURL());
    }

    private void recreateHandler(BlackboardAlertHandler handler,
            ExternalAlertSource src, URL url) throws Exception {
        handler.poller = new BlackboardCapFeedPoller(url);
        synchronized (this) {
            if (this.handlers.get(url) != null) {
                throw new Exception("Blackboard URL " + url + " already registered");
            }
            this.handlers.put(url, handler);
        }
        handler.poller.setHandler(this.FeedHandler);
        handler.future = handler.threadpool
                .scheduleWithFixedDelay(handler.poller, 5, 5, TimeUnit.SECONDS);
    }


    private final class BlackboardAlertHandler implements ExternalAlertHandler {
        private final ExternalAlertManager extAlrtMngr;
        private BlackboardCapFeedPoller poller;
        private ExternalAlertSource source;

        private ScheduledExecutorService threadpool;
        private ScheduledFuture future;

        private BlackboardAlertHandler(final ExternalAlertManager mngr,
                final BlackboardCapFeedPoller poller,
                final ExternalAlertSource src) {
            this.extAlrtMngr = mngr;
            this.poller = poller;
            this.source = src;
        }

        @Override
        public ExternalAlertSource getSource() {
            return this.source;
        }

        public URL getURL() {
            return this.poller.getURL();
        }

        @Override
        public void update(ExternalAlertSource src) throws Exception {
            URL url = new URL(src.getSource());
            if (url.equals(this.poller.getURL())) {
                // URL has changed, need to create a new poller entity
                removeHandler(this);
                this.source = src;
                recreateHandler(this, src, url);
            } else {
                // same URL/feed, just update our source definition
                this.source = src;
            }
        }

        @Override
        public void shutdown() {
            if (this.future != null) {
                this.future.cancel(true);
            }
        }

        private DispatchArguments getDispatchArguments() {
            return this.extAlrtMngr.applyDispatchDefaults(this.source);
        }
    };

    private final BlackboardCapFeedPoller.Handler FeedHandler
            = new BlackboardCapFeedPoller.Handler() {
        @Override
        public void handleEntryAdded(BlackboardCapFeedPoller feed, String url, Alert cap) {
            if (cap.getInfoCount() > 0) {
                BlackboardAlertHandler handler;
                synchronized (BlackboardAlertSourceType.this) {
                    handler = handlers.get(feed.getURL());
                }

                DispatchArguments dispatchArgs;
                if (handler != null) {
                    dispatchArgs = handler.getDispatchArguments();
                } else {
                    dispatchArgs = new DispatchArguments();
                }
                if (dispatchArgs.source == null) {
                    dispatchArgs.source = "Blackboard[" + feed.getName() + "]";
                }

                // TODO: apply feed specific alert override CAP-field mappings, group mappings
                dispatchArgs.alert = capUtils.alertFromCap(dispatchArgs.alert, cap.getInfo(0));

                // dispatch the alert
                Dispatch dispatch = dispatchManager.dispatchAlert(
                        dispatchArgs.source, dispatchArgs.alert,
                        dispatchArgs.groups, dispatchArgs.options);
                synchronized (BlackboardAlertSourceType.this) {
                    dispatches.put(url, dispatch);
                }
            }
        }

        @Override
        public void handleEntryRemoved(BlackboardCapFeedPoller feed, String url) {
            Dispatch dispatch;
            synchronized (BlackboardAlertSourceType.this) {
                dispatch = dispatches.remove(url);
            }
            if (dispatch != null) {
                DateTime end = dispatch.getEndTime();
                if (end == null || end.minusSeconds(15).isAfterNow()) {
                    // dispatch still active (also end is not imminent), cancel
                    dispatchManager.cancelAlert("Blackboard[" + feed.getName() + "]",
                            dispatch, null, null);
                }
            }
        }
    };

}
