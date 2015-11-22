/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.net.Requester;
import cn.edu.hfut.dmic.webcollector.util.Config;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * deep web抓取器
 *
 * @author hu
 */
public class Fetcher {

    public static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

    public DBManager dbManager;

    public Requester requester;

    public Visitor visitor;

    private AtomicInteger activeThreads;
    private AtomicInteger spinWaiting;
    private AtomicLong lastRequestStart;
    private QueueFeeder feeder;
    private FetchQueue fetchQueue;
    private int retry = 3;
    private long retryInterval=0;
    private long visitInterval=0;

    /**
     *
     */
    public static final int FETCH_SUCCESS = 1;

    /**
     *
     */
    public static final int FETCH_FAILED = 2;
    private int threads = 50;
    private boolean isContentStored = false;

    public Visitor getVisitor() {
        return visitor;
    }

    public void setVisitor(Visitor visitor) {
        this.visitor = visitor;
    }

    /**
     *
     */
    public static class FetchItem {

        /**
         *
         */
        public CrawlDatum datum;

        /**
         *
         * @param datum
         */
        public FetchItem(CrawlDatum datum) {
            this.datum = datum;
        }
    }

    /**
     *
     */
    public static class FetchQueue {

        /**
         *
         */
        public AtomicInteger totalSize = new AtomicInteger(0);

        /**
         *
         */
        public final List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());

        /**
         *
         */
        public void clear() {
            queue.clear();
        }

        /**
         *
         * @return
         */
        public int getSize() {
            return queue.size();
        }

        /**
         *
         * @param item
         */
        public synchronized void addFetchItem(FetchItem item) {
            if (item == null) {
                return;
            }
            queue.add(item);
            totalSize.incrementAndGet();
        }

        /**
         *
         * @return
         */
        public synchronized FetchItem getFetchItem() {
            if (queue.isEmpty()) {
                return null;
            }
            return queue.remove(0);
        }

        /**
         *
         */
        public synchronized void dump() {
            for (int i = 0; i < queue.size(); i++) {
                FetchItem it = queue.get(i);
                LOG.info("  " + i + ". " + it.datum.getUrl());
            }

        }

    }

    /**
     *
     */
    public static class QueueFeeder extends Thread {

        /**
         *
         */
        public FetchQueue queue;

        /**
         *
         */
        public Generator generator;

        /**
         *
         */
        public int size;

        /**
         *
         * @param queue
         * @param generator
         * @param size
         */
        public QueueFeeder(FetchQueue queue, Generator generator, int size) {
            this.queue = queue;
            this.generator = generator;
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

                    CrawlDatum datum = generator.next();
                    hasMore = (datum != null);

                    if (hasMore) {
                        queue.addFetchItem(new FetchItem(datum));
                        feed--;
                    }

                }

            }
            try {
                generator.close();
            } catch (Exception ex) {
                LOG.info("Exception when closing generator", ex);
            }

        }

    }

    private class FetcherThread extends Thread {

        @Override
        public void run() {
            activeThreads.incrementAndGet();
            FetchItem item = null;
            try {

                while (running) {
                    try {
                        item = fetchQueue.getFetchItem();
                        if (item == null) {
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

                        CrawlDatum crawlDatum = item.datum;
                        String url = crawlDatum.getUrl();
                        Page page = getPage(crawlDatum);

                        crawlDatum.incrRetry(page.getRetry());
                        crawlDatum.setFetchTime(System.currentTimeMillis());

                        CrawlDatums next = new CrawlDatums();
                        if (visit(crawlDatum, page, next)) {
                            try {
                                /*写入fetch信息*/
                                dbManager.wrtieFetchSegment(crawlDatum);
                                if (page.getResponse() == null) {
                                    continue;
                                }
                                if (page.getResponse().isRedirect()) {
                                    if (page.getResponse().getRealUrl() != null) {
                                        dbManager.writeRedirectSegment(crawlDatum, page.getResponse().getRealUrl().toString());
                                    }
                                }
                                if (!next.isEmpty()) {
                                    dbManager.wrtieParseSegment(next);
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

    private void before() throws Exception {
        //DbUpdater recoverDbUpdater = createRecoverDbUpdater();

        try {

            if (dbManager.isLocked()) {
                dbManager.merge();
                dbManager.unlock();
            }

        } catch (Exception ex) {
            LOG.info("Exception", ex);
        }

        dbManager.lock();
        dbManager.initSegmentWriter();
        running = true;
    }

    /**
     * 抓取当前所有任务，会阻塞到爬取完成
     *
     * @param generator 给抓取提供任务的Generator(抓取任务生成器)
     * @throws IOException
     */
    public void fetchAll(Generator generator) throws Exception {
        if (visitor == null) {
            LOG.info("Please specify a Visitor!");
            return;
        }
        before();

        lastRequestStart = new AtomicLong(System.currentTimeMillis());

        activeThreads = new AtomicInteger(0);
        spinWaiting = new AtomicInteger(0);
        fetchQueue = new FetchQueue();
        feeder = new QueueFeeder(fetchQueue, generator, 1000);
        feeder.start();

        FetcherThread[] fetcherThreads = new FetcherThread[threads];
        for (int i = 0; i < threads; i++) {
            fetcherThreads[i] = new FetcherThread();
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
        after();

    }

    boolean running;

    /**
     * 停止爬取
     */
    public void stop() {
        running = false;
    }

    private void after() throws Exception {

        dbManager.closeSegmentWriter();
        dbManager.merge();
        dbManager.unlock();

    }

    /**
     * 返回爬虫的线程数
     *
     * @return 爬虫的线程数
     */
    public int getThreads() {
        return threads;
    }

    /**
     * 设置爬虫的线程数
     *
     * @param threads 爬虫的线程数
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * 返回是否存储网页/文件的内容
     *
     * @return 是否存储网页/文件的内容
     */
    public boolean isIsContentStored() {
        return isContentStored;
    }

    /**
     * 设置是否存储网页／文件的内容
     *
     * @param isContentStored 是否存储网页/文件的内容
     */
    public void setIsContentStored(boolean isContentStored) {
        this.isContentStored = isContentStored;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public DBManager getDBManager() {
        return dbManager;
    }

    public void setDBManager(DBManager dbManager) {
        this.dbManager = dbManager;
    }

    public Requester getRequester() {
        return requester;
    }

    public void setRequester(Requester requester) {
        this.requester = requester;
    }

    public boolean visit(CrawlDatum crawlDatum, Page page, CrawlDatums next) {
        String url = crawlDatum.getUrl();
        if (page.getStatus() == Page.STATUS_FETCH_SUCCESS) {

            crawlDatum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
            crawlDatum.setHttpCode(page.getResponse().getCode());

            try {
                visitor.visit(page, next);
            } catch (Exception ex) {
                LOG.info("Exception when visit URL: " + url, ex);
                return false;
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
            LOG.info("fetch URL: " + url);
            page = Page.createSuccessPage(crawlDatum, retryCount, response);
        } else {
            LOG.info("failed URL: " + url + " (" + lastException+")");
            page = Page.createFailedPage(crawlDatum, retryCount, lastException);
        }

        return page;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public long getVisitInterval() {
        return visitInterval;
    }

    public void setVisitInterval(long visitInterval) {
        this.visitInterval = visitInterval;
    }
    
    

}
