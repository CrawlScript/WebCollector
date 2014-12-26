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
package cn.edu.hfut.dmic.webcollector.deepcrawler;

import cn.edu.hfut.dmic.webcollector.fetcher.*;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.Generator;

import cn.edu.hfut.dmic.webcollector.model.Content;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.Request;
import cn.edu.hfut.dmic.webcollector.net.RequestFactory;
import cn.edu.hfut.dmic.webcollector.net.Response;
import cn.edu.hfut.dmic.webcollector.parser.ParseData;
import cn.edu.hfut.dmic.webcollector.parser.ParseResult;

import cn.edu.hfut.dmic.webcollector.util.Config;

import cn.edu.hfut.dmic.webcollector.util.LogUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * deep web抓取器
 *
 * @author hu
 */
public class DeepFetcher {

    /**
     *
     */
    public DbUpdater dbUpdater = null;

    /**
     *
     */
    public RequestFactory requestFactory = null;

    public VisitorFactory visitorFactory = null;

    /**
     *
     */
    private int retry = 3;
    private AtomicInteger activeThreads;
    private AtomicInteger spinWaiting;
    private AtomicLong lastRequestStart;
    private QueueFeeder feeder;
    private FetchQueue fetchQueue;

    /**
     *
     */
    public static final int FETCH_SUCCESS = 1;

    /**
     *
     */
    public static final int FETCH_FAILED = 2;
    private int threads = 10;
    private boolean isContentStored = false;

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
        public List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());

        /**
         *
         */
        public synchronized void clear() {
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
        public void addFetchItem(FetchItem item) {
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
                LogUtils.getLogger().info("  " + i + ". " + it.datum.getUrl());
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

        @Override
        public void run() {

            boolean hasMore = true;
            while (hasMore) {

                int feed = size - queue.getSize();
                if (feed <= 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }
                    continue;
                }
                while (feed > 0 && hasMore) {

                    CrawlDatum datum = generator.next();
                    hasMore = (datum != null);

                    if (hasMore) {
                        queue.addFetchItem(new FetchItem(datum));
                        feed--;
                    }

                }

            }

        }

    }

    private class FetcherThread extends Thread {

        @Override
        public void run() {
            activeThreads.incrementAndGet();
            FetchItem item = null;
            try {

                while (true) {
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

                        CrawlDatum crawldatum = new CrawlDatum();
                        String url = item.datum.getUrl();
                        crawldatum.setUrl(url);

                        Request request = requestFactory.createRequest(url);
                        Response response = null;

                        for (int i = 0; i <= retry; i++) {
                            if (i > 0) {
                                LogUtils.getLogger().info("retry " + i + "th " + url);
                            }
                            try {
                                response = request.getResponse(crawldatum);
                                break;
                            } catch (Exception ex) {

                            }
                        }

                        crawldatum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
                        crawldatum.setFetchTime(System.currentTimeMillis());

                        Page page = new Page();
                        page.setUrl(url);
                        page.setFetchTime(crawldatum.getFetchTime());
                        page.setResponse(response);

                        if (response == null) {
                            LogUtils.getLogger().info("failed " + url);
                            continue;
                        }

                        LogUtils.getLogger().info("fetch " + url);
                        String contentType = response.getContentType();
                        Visitor visitor = visitorFactory.createVisitor(url, contentType);

                        try {
                            /*写入fetch信息*/
                            dbUpdater.getSegmentWriter().wrtieFetch(crawldatum);
                            /*写入content信息*/
                            if (isContentStored) {
                                Content content = new Content();
                                content.setUrl(url);
                                if (response.getContent() != null) {
                                    content.setContent(response.getContent());
                                } else {
                                    content.setContent(new byte[0]);
                                }
                                content.setContentType(contentType);
                                dbUpdater.getSegmentWriter().wrtieContent(content);
                            }
                            if (visitor != null) {
                                ArrayList<String> nextLinks = null;
                                try {
                                    /*用户自定义visitor处理页面,并获取链接*/
                                    nextLinks = visitor.visitAndGetNextLinks(page);

                                } catch (Exception ex) {
                                    LogUtils.getLogger().info("Exception", ex);
                                }

                                /*写入解析出的链接*/
                                if (nextLinks != null && !nextLinks.isEmpty()) {
                                    ArrayList<Link> parseLinks = new ArrayList<Link>();
                                    for (String nextLink : nextLinks) {
                                        if (nextLink != null) {
                                            parseLinks.add(new Link(null, nextLink));
                                        }
                                    }
                                    ParseData parseData = new ParseData(url, parseLinks);
                                    dbUpdater.getSegmentWriter().wrtieParse(new ParseResult(parseData));
                                }
                            }

                        } catch (Exception ex) {
                            LogUtils.getLogger().info("Exception", ex);

                        }

                    } catch (Exception ex) {
                        LogUtils.getLogger().info("Exception", ex);
                    }
                }

            } catch (Exception ex) {
                LogUtils.getLogger().info("Exception", ex);

            } finally {
                activeThreads.decrementAndGet();
            }

        }

    }

    private void before() throws Exception {
        //DbUpdater recoverDbUpdater = createRecoverDbUpdater();

        try {

            if (dbUpdater.isLocked()) {
                dbUpdater.merge();
                dbUpdater.unlock();
            }

        } catch (Exception ex) {
            LogUtils.getLogger().info("Exception", ex);
        }

        dbUpdater.initSegmentWriter();
        dbUpdater.lock();

        running = true;
    }

    /**
     * 抓取当前所有任务，会阻塞到爬取完成
     *
     * @param generator 给抓取提供任务的Generator(抓取任务生成器)
     * @throws IOException
     */
    public void fetchAll(Generator generator) throws Exception {
        if (visitorFactory == null) {
            LogUtils.getLogger().info("Please specify a VisitorFactory!");
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
            LogUtils.getLogger().info("-activeThreads=" + activeThreads.get()
                    + ", spinWaiting=" + spinWaiting.get() + ", fetchQueue.size="
                    + fetchQueue.getSize());

            if (!feeder.isAlive() && fetchQueue.getSize() < 5) {
                fetchQueue.dump();
            }

            if ((System.currentTimeMillis() - lastRequestStart.get()) > Config.requestMaxInterval) {
                LogUtils.getLogger().info("Aborting with " + activeThreads + " hung threads.");
                break;
            }

        } while (activeThreads.get() > 0 && running);

        for (int i = 0; i < threads; i++) {
            if (fetcherThreads[i].isAlive()) {
                fetcherThreads[i].stop();
            }
        }
        feeder.stop();
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

        dbUpdater.close();
        dbUpdater.merge();
        dbUpdater.unlock();

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
     * 返回http请求失败后重试的次数
     *
     * @return http请求失败后重试的次数
     */
    public int getRetry() {
        return retry;
    }

    /**
     * 设置http请求失败后重试的次数
     *
     * @param retry http请求失败后重试的次数
     */
    public void setRetry(int retry) {
        this.retry = retry;
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

    /**
     * 返回CrawlDB更新器
     *
     * @return CrawlDB更新器
     */
    public DbUpdater getDbUpdater() {
        return dbUpdater;
    }

    /**
     * 设置CrawlDB更新器
     *
     * @param dbUpdater CrawlDB更新器
     */
    public void setDbUpdater(DbUpdater dbUpdater) {
        this.dbUpdater = dbUpdater;
    }

    /**
     * 返回请求生成器
     *
     * @return 请求生成器
     */
    public RequestFactory getRequestFactory() {
        return requestFactory;
    }

    /**
     * 设置请求生成器
     *
     * @param requestFactory 请求生成器
     */
    public void setRequestFactory(RequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }

    public VisitorFactory getVisitorFactory() {
        return visitorFactory;
    }

    public void setVisitorFactory(VisitorFactory visitorFactory) {
        this.visitorFactory = visitorFactory;
    }

}
