/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.model.Content;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.model.ParseData;
import cn.edu.hfut.dmic.webcollector.model.Redirect;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.net.Requester;
import cn.edu.hfut.dmic.webcollector.plugin.Plugin;
import cn.edu.hfut.dmic.webcollector.plugin.common.CommonRequester;
import cn.edu.hfut.dmic.webcollector.util.Config;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class FetcherReducer extends Reducer<Text, CrawlDatum, Text, Writable> {

    public static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

    private AtomicInteger activeThreads;
    private AtomicInteger spinWaiting;
    private AtomicLong lastRequestStart;
    private QueueFeeder feeder;
    private FetchQueue fetchQueue;
    private int retry = 3;
    private long retryInterval = 0;
    private long visitInterval = 0;
    boolean running;
    int threads = 50;

    private Requester requester;
    private Visitor visitor;

    public static class FetchQueue {

        public AtomicInteger totalSize = new AtomicInteger(0);

        public final List<CrawlDatum> queue = Collections.synchronizedList(new LinkedList<CrawlDatum>());

        public void clear() {
            queue.clear();
        }

        public int getSize() {
            return queue.size();
        }

        public synchronized void addDatum(CrawlDatum item) {
            if (item == null) {
                return;
            }
            queue.add(item);
            totalSize.incrementAndGet();
        }

        public synchronized CrawlDatum getDatum() {
            if (queue.isEmpty()) {
                return null;
            }
            return queue.remove(0);
        }

        public synchronized void dump() {
            for (int i = 0; i < queue.size(); i++) {
                CrawlDatum item = queue.get(i);
                LOG.info("  " + i + ". " + item.getKey());
            }

        }
    }

    public static class QueueFeeder extends Thread {

        public FetchQueue queue;

        public Context context;

        public int size;

        public QueueFeeder(FetchQueue queue, Context context, int size) {
            this.queue = queue;
            this.context = context;
            this.size = size;
        }

        public void stopFeeder() {
            running = false;
            while (this.isAlive()) {
                try {
                    Thread.sleep(1000);
                    LOG.info("stopping feeder......");
                } catch (InterruptedException ex) {
                }
            }
        }
        public boolean running = true;

        @Override
        public void run() {

            boolean hasMore = true;
            running = true;
            int cnt = 0;
            try {
                Iterator<CrawlDatum> ite;
                while (hasMore && running) {

                    int feed = size - queue.getSize();
                    if (feed <= 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                        }
                        continue;
                    }

                    while (feed > 0 && hasMore && running) {
                        hasMore = context.nextKey();
                        if (hasMore) {
                            ite = context.getValues().iterator();
                            CrawlDatum copy = ite.next().copy();
                            queue.addDatum(copy);
                            feed--;
                        }
                    }

                }
            } catch (Exception ex) {
                LOG.error("QueueFeeder error reading input, record " + cnt, ex);
                return;
            }

        }

    }

    private class FetcherThread extends Thread {

        Context context;

        public FetcherThread(Context context) {
            this.context = context;
        }

        @Override
        public void run() {
            activeThreads.incrementAndGet();
            CrawlDatum datum;
            try {

                while (running) {
                    try {
                        datum = fetchQueue.getDatum();
                        if (datum == null) {
                            if (feeder.isAlive() || fetchQueue.getSize() > 0) {
                                spinWaiting.incrementAndGet();

                                try {
                                    Thread.sleep(500);
                                } catch (Exception ex) {
                                }

                                spinWaiting.decrementAndGet();
                                continue;
                            } else {
                                return;
                            }
                        }

                        lastRequestStart.set(System.currentTimeMillis());

                        String url = datum.getUrl();
                        Page page = getPage(datum);

                        datum.incrRetry(page.getRetry());
                        datum.setFetchTime(System.currentTimeMillis());

                        CrawlDatums next = new CrawlDatums();
                        if (visit(datum, page, next)) {
                            try {
                                Text key = new Text(datum.getKey());
                                context.write(key, datum);
                                if (page.getResponse() == null) {
                                    continue;
                                }
                                if (page.getResponse().isRedirect()) {
                                    if (page.getResponse().getRealUrl() != null) {
                                        context.write(key, new Redirect(datum, page.getResponse().getRealUrl().toString()));
                                    }
                                }
                                if (page.getResponse().getContent() != null) {
                                    Content content = new Content(datum.getKey(), page.getResponse().getContent());
                                    context.write(key, content);
                                }

                                if (!next.isEmpty()) {
                                    context.write(key, new ParseData(next));
                                }

                            } catch (Exception ex) {
                                LOG.info("Exception when updating db", ex);
                            }
                        }
                        if (visitInterval > 0) {
                            try {
                                Thread.sleep(visitInterval);
                            } catch (Exception sleepEx) {
                            }
                        }

                    } catch (Exception ex) {
                        LOG.info("Exception", ex);
                    }
                }

            } catch (Exception ex) {
                LOG.info("Exception", ex);

            } finally {
                activeThreads.decrementAndGet();
            }

        }

    }

    public boolean visit(CrawlDatum crawlDatum, Page page, CrawlDatums next) {
        String url = crawlDatum.getUrl();
        if (page.getStatus() == Page.STATUS_FETCH_SUCCESS) {

            crawlDatum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
            crawlDatum.setHttpCode(page.getResponse().getCode());

            if (!page.getResponse().isNotFound()) {

                try {
                    visitor.visit(page, next);
                } catch (Exception ex) {
                    LOG.info("Exception when visit URL: " + url, ex);
                    return false;
                }
            } else {
                try {
                    visitor.notFound(page, next);
                } catch (Exception ex) {
                    LOG.info("Exception when not found URL: " + url, ex);
                    return false;
                }
            }

            try {
                visitor.afterVisit(page, next);
            } catch (Exception ex) {
                LOG.info("Exception after visit URL: " + url, ex);
                return false;
            }

        } else if (page.getStatus() == Page.STATUS_FETCH_FAILED) {

            crawlDatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);

            try {
                visitor.fail(page, next);
            } catch (Exception ex) {
                LOG.info("Exception when execute failed URL: " + url, ex);
                return false;
            }
        }
        return true;
    }

    public Page getPage(CrawlDatum crawlDatum) {

        String url = crawlDatum.getUrl();
        Page page;
        HttpResponse response = null;
        int retryIndex = 0;
        Exception lastException = null;
        int retryCount = 0;
        for (; retryIndex <= retry; retryIndex++) {
            try {
                response = requester.getResponse(crawlDatum);//this.getHttpResponse(crawlDatum);
                break;
            } catch (Exception ex) {

                String suffix = "th ";
                switch (retryIndex + 1) {
                    case 1:
                        suffix = "st ";
                        break;
                    case 2:
                        suffix = "nd ";
                        break;
                    case 3:
                        suffix = "rd ";
                        break;
                    default:
                        suffix = "th ";
                }

                lastException = ex;

                if (retryIndex < retry) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("retry ").append(retryIndex + 1).append(suffix).append("URL:")
                            .append(url).append(" after ").append(retryInterval)
                            .append("ms ").append("(").append(ex.toString()).append(")");
                    String logMessage = sb.toString();
                    LOG.info(logMessage);
                    retryCount++;
                    if (retryInterval > 0) {
                        try {
                            Thread.sleep(retryInterval);
                        } catch (Exception sleepEx) {
                        }
                    }
                }

            }
        }

        if (response != null) {
            if (!response.isNotFound()) {
                LOG.info("fetch URL: " + url);
            } else {
                //404应该被当作抓取成功，因为404告诉爬虫页面不存在，以后不需要重试页面
                LOG.info("ignore URL: " + url + " (not found)");
            }
            page = Page.createSuccessPage(crawlDatum, retryCount, response);

        } else {
            LOG.info("failed URL: " + url + " (" + lastException + ")");
            page = Page.createFailedPage(crawlDatum, retryCount, lastException);
        }

        return page;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String requesterClass = conf.get("requester.class", CommonRequester.class.getName());
        try {
            requester = Plugin.<Requester>createPlugin(requesterClass);
        } catch (Exception ex) {
            LOG.info("Exception when initializing requester " + requesterClass, ex);
            return;
        }
        String visitorClass = conf.get("visitor.class");
        if (visitorClass == null) {
            LOG.info("Must specify a visitor!");
            return;
        }
        try {
            visitor = Plugin.<Visitor>createPlugin(visitorClass);
        } catch (Exception ex) {
            LOG.info("Exception when initializing visitor " + visitorClass, ex);
            return;
        }

        try {
            running = true;

            lastRequestStart = new AtomicLong(System.currentTimeMillis());

            activeThreads = new AtomicInteger(0);
            spinWaiting = new AtomicInteger(0);
            fetchQueue = new FetchQueue();
            feeder = new QueueFeeder(fetchQueue, context, 1000);
            feeder.start();

            FetcherThread[] fetcherThreads = new FetcherThread[threads];
            for (int i = 0; i < threads; i++) {
                fetcherThreads[i] = new FetcherThread(context);
                fetcherThreads[i].start();
            }

            do {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                LOG.info("-activeThreads=" + activeThreads.get()
                        + ", spinWaiting=" + spinWaiting.get() + ", fetchQueue.size="
                        + fetchQueue.getSize());

                if (!feeder.isAlive() && fetchQueue.getSize() < 5) {
                    fetchQueue.dump();
                }

                if ((System.currentTimeMillis() - lastRequestStart.get()) > Config.THREAD_KILLER) {
                    LOG.info("Aborting with " + activeThreads + " hung threads.");
                    break;
                }

            } while (activeThreads.get() > 0 && running);
            running = false;
            long waitThreadEndStartTime = System.currentTimeMillis();
            if (activeThreads.get() > 0) {
                LOG.info("wait for activeThreads to end");
            }
            /*等待存活线程结束*/
            while (activeThreads.get() > 0) {
                LOG.info("-activeThreads=" + activeThreads.get());
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                }
                if (System.currentTimeMillis() - waitThreadEndStartTime > Config.WAIT_THREAD_END_TIME) {
                    LOG.info("kill threads");
                    for (int i = 0; i < fetcherThreads.length; i++) {
                        if (fetcherThreads[i].isAlive()) {
                            try {
                                fetcherThreads[i].stop();
                                LOG.info("kill thread " + i);
                            } catch (Exception ex) {
                                LOG.info("Exception", ex);
                            }
                        }
                    }
                    break;
                }
            }
            LOG.info("clear all activeThread");
            feeder.stopFeeder();
            fetchQueue.clear();
        } finally {

        }
    }

}
