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
package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.fetcher.Visitor;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.net.Requester;
import cn.edu.hfut.dmic.webcollector.util.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public abstract class Crawler {

    public static final Logger LOG = LoggerFactory.getLogger(Crawler.class);

    protected int status;
    public final static int RUNNING = 1;
    public final static int STOPED = 2;
    protected boolean resumable = false;
    protected int threads = 50;

    protected int topN = -1;
    protected int retry = 3;
    protected long retryInterval = 0;
    protected long visitInterval = 0;

    protected CrawlDatums seeds = new CrawlDatums();
    protected CrawlDatums forcedSeeds = new CrawlDatums();
    protected Fetcher fetcher;
    protected int maxRetry = -1;

    protected Requester requester;
    protected Visitor visitor;
    protected DBManager dbManager;
    protected Generator generator;

    protected void inject() throws Exception {
        dbManager.inject(seeds);
    }

    public void injectForcedSeeds() throws Exception {
        dbManager.inject(forcedSeeds);
    }

    public void start(int depth) throws Exception {

        boolean needInject = true;

        if (resumable && dbManager.isDBExists()) {
            needInject = false;
        }

        if (!resumable) {
            if (dbManager.isDBExists()) {
                dbManager.clear();
            }

            if (seeds.isEmpty() && forcedSeeds.isEmpty()) {
                LOG.info("error:Please add at least one seed");
                return;
            }

        }
        dbManager.open();

        if (needInject) {
            inject();
        }

        if (!forcedSeeds.isEmpty()) {
            injectForcedSeeds();
        }

        status = RUNNING;
        for (int i = 0; i < depth; i++) {
            if (status == STOPED) {
                break;
            }
            LOG.info("start depth " + (i + 1));
            long startTime = System.currentTimeMillis();

            if (maxRetry >= 0) {
                generator.setMaxRetry(maxRetry);
            } else {
                generator.setMaxRetry(Config.MAX_RETRY);
            }
            generator.setTopN(topN);
            fetcher = new Fetcher();
            fetcher.setRetryInterval(retryInterval);
            fetcher.setVisitInterval(visitInterval);
            fetcher.setDBManager(dbManager);
            fetcher.setRequester(requester);
            fetcher.setVisitor(visitor);
            fetcher.setRetry(retry);
            fetcher.setThreads(threads);
            fetcher.fetchAll(generator);
            long endTime = System.currentTimeMillis();
            long costTime = (endTime - startTime) / 1000;
            int totalGenerate = generator.getTotalGenerate();

            LOG.info("depth " + (i + 1) + " finish: \n\tTOTAL urls:\t" + totalGenerate + "\n\tTOTAL time:\t" + costTime + " seconds");
            if (totalGenerate == 0) {
                break;
            }

        }
        dbManager.close();
    }

    public void stop() {
        status = STOPED;
        fetcher.stop();
    }

    public void addSeed(CrawlDatum datum, boolean force) {
        if (force) {
            forcedSeeds.add(datum);
        } else {
            seeds.add(datum);
        }
    }

    public void addSeed(CrawlDatum datum) {
        addSeed(datum, false);
    }

    public void addSeed(CrawlDatums datums, boolean force) {
        for (CrawlDatum datum : datums) {
            addSeed(datum, force);
        }
    }

    public void addSeed(CrawlDatums datums) {
        addSeed(datums, false);
    }

    public void addSeed(Links links, boolean force) {
        for (String url : links) {
            addSeed(url, force);
        }
    }

    public void addSeed(Links links) {
        addSeed(links, false);
    }

    public void addSeed(String url, boolean force) {
        CrawlDatum datum = new CrawlDatum(url);
        addSeed(datum, force);
    }

    public void addSeed(String url) {
        addSeed(url, false);
    }

    public boolean isResumable() {
        return resumable;
    }

    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public Requester getRequester() {
        return requester;
    }

    public void setRequester(Requester requester) {
        this.requester = requester;
    }

    public Visitor getVisitor() {
        return visitor;
    }

    public void setVisitor(Visitor visitor) {
        this.visitor = visitor;
    }

    public Generator getGenerator() {
        return generator;
    }

    public void setGenerator(Generator generator) {
        this.generator = generator;
    }

    public int getTopN() {
        return topN;
    }

    public void setTopN(int topN) {
        this.topN = topN;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
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

    public DBManager getDbManager() {
        return dbManager;
    }

    public void setDbManager(DBManager dbManager) {
        this.dbManager = dbManager;
    }

}
