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

import cn.edu.hfut.dmic.webcollector.generator.StandardGenerator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpRequester;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.util.Config;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * deep web抓取器
 *
 * @author hu
 */
public class Fetcher {

    public static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

    public DbUpdater dbUpdater = null;

    public HttpRequester httpRequester = null;

    public VisitorFactory visitorFactory = null;

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
    private int threads = 50;
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
        public StandardGenerator generator;

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
        public QueueFeeder(FetchQueue queue, StandardGenerator generator, int size) {
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
            generator.close();

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

                        String url = item.datum.getUrl();

                        HttpResponse response = null;
                        int retryCount = 0;
                        for (; retryCount <= retry; retryCount++) {
                            if (retryCount > 0) {
                                LOG.info("retry " + retryCount + "th " + url);
                            }
                            try {
                                response = httpRequester.getResponse(url);
                                break;
                            } catch (Exception ex) {
                                String logMessage="fetch of "+url+" failed once with "+ex.getMessage();
                                if(retryCount<retry){
                                    logMessage+="   retry";
                                }
                                LOG.info(logMessage);
                            }
                        }
                        CrawlDatum crawlDatum = null;
                        if (response != null) {
                            LOG.info("fetch " + url);
                            crawlDatum = new CrawlDatum(url, CrawlDatum.STATUS_DB_FETCHED, item.datum.getRetry() + retryCount);
                        } else {
                            LOG.info("failed " + url);
                            crawlDatum = new CrawlDatum(url, CrawlDatum.STATUS_DB_UNFETCHED, item.datum.getRetry() + retryCount);
                        }

                        try {
                            /*写入fetch信息*/
                            long fetchStart = System.currentTimeMillis();
                            dbUpdater.getSegmentWriter().wrtieFetch(crawlDatum);
                            long fetchEnd = System.currentTimeMillis();
                            LOG.debug("writing fetch elapse " + (fetchEnd - fetchStart));
                            if (response == null) {
                                continue;
                            }
                            if(response.getRedirect()){
                                if(response.getRealUrl()!=null){
                                    dbUpdater.getSegmentWriter().writeRedirect(response.getUrl(), response.getRealUrl());
                                }
                            }
                            String contentType = response.getContentType();
                            Visitor visitor = visitorFactory.createVisitor(url, contentType);

                            Page page = new Page();
                            page.setUrl(url);
                            page.setResponse(response);
                            if (visitor != null) {
                                Links nextLinks = null;
                                try {
                                    /*用户自定义visitor处理页面,并获取链接*/
                                    nextLinks = visitor.visitAndGetNextLinks(page);

                                } catch (Exception ex) {
                                    LOG.info("Exception", ex);
                                }

                                /*写入解析出的链接*/
                                if (nextLinks != null && !nextLinks.isEmpty()) {
                                    long linkStart = System.currentTimeMillis();
                                    dbUpdater.getSegmentWriter().wrtieLinks(nextLinks);
                                    long linkEnd = System.currentTimeMillis();
                                    LOG.debug("writing link elapse " + (linkEnd - linkStart));
                                }
                            }

                        } catch (Exception ex) {
                            LOG.info("Exception", ex);

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

            if (dbUpdater.isLocked()) {
                dbUpdater.merge();
                dbUpdater.unlock();
            }

        } catch (Exception ex) {
            LOG.info("Exception", ex);
        }

        dbUpdater.lock();
        dbUpdater.getSegmentWriter().init();
        running = true;
    }

    /**
     * 抓取当前所有任务，会阻塞到爬取完成
     *
     * @param generator 给抓取提供任务的Generator(抓取任务生成器)
     * @throws IOException
     */
    public void fetchAll(StandardGenerator generator) throws Exception {
        if (visitorFactory == null) {
            LOG.info("Please specify a VisitorFactory!");
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

            if ((System.currentTimeMillis() - lastRequestStart.get()) > Config.requestMaxInterval) {
                LOG.info("Aborting with " + activeThreads + " hung threads.");
                break;
            }

        } while (activeThreads.get() > 0 && running);
        running=false;
        long waitThreadEndStartTime=System.currentTimeMillis();
        if(activeThreads.get()>0){
            LOG.info("wait for activeThreads to end");
        }
        /*等待存活线程结束*/
        while (activeThreads.get()>0) {
            LOG.info("-activeThreads="+activeThreads.get());
            try{
                Thread.sleep(500);
            }catch(Exception ex){}
            if(System.currentTimeMillis()-waitThreadEndStartTime>Config.WAIT_THREAD_END_TIME){
                LOG.info("kill threads");
                for(int i=0;i<fetcherThreads.length;i++){
                    if(fetcherThreads[i].isAlive()){
                        try{
                            fetcherThreads[i].stop();
                            LOG.info("kill thread "+i);
                        }catch(Exception ex){
                            LOG.info("Exception",ex);
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

    public HttpRequester getHttpRequester() {
        return httpRequester;
    }

    public void setHttpRequester(HttpRequester httpRequester) {
        this.httpRequester = httpRequester;
    }

    public VisitorFactory getVisitorFactory() {
        return visitorFactory;
    }

    public void setVisitorFactory(VisitorFactory visitorFactory) {
        this.visitorFactory = visitorFactory;
    }

}
