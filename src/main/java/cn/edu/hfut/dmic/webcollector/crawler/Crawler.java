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

import cn.edu.hfut.dmic.webcollector.conf.DefaultConfigured;
import cn.edu.hfut.dmic.webcollector.crawldb.GeneratorFilter;
import cn.edu.hfut.dmic.webcollector.crawldb.StatusGeneratorFilter;
import cn.edu.hfut.dmic.webcollector.fetcher.NextFilter;
import cn.edu.hfut.dmic.webcollector.fetcher.Executor;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;

import cn.edu.hfut.dmic.webcollector.util.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hu
 */
public class Crawler extends DefaultConfigured {

    public static final Logger LOG = LoggerFactory.getLogger(Crawler.class);

    public Crawler() {

    }

    /**
     * 根据任务管理器和执行器构造爬虫
     *
     * @param dbManager 任务管理器
     * @param executor 执行器
     */
    public Crawler(DBManager dbManager, Executor executor) {
        this.dbManager = dbManager;
        this.executor = executor;
    }

    protected int status;
    public final static int RUNNING = 1;
    public final static int STOPED = 2;
    protected boolean resumable = false;
    protected int threads = 50;


    protected CrawlDatums seeds = new CrawlDatums();
    protected CrawlDatums forcedSeeds = new CrawlDatums();
    protected Fetcher fetcher;
    protected int maxExecuteCount = -1;

    protected Executor executor = null;
    protected NextFilter nextFilter = null;
    protected DBManager dbManager;
    protected GeneratorFilter generatorFilter = new StatusGeneratorFilter();
    protected void inject() throws Exception {
        dbManager.inject(seeds);
    }

    protected void injectForcedSeeds() throws Exception {
        dbManager.inject(forcedSeeds, true);
    }


    protected void registerOtherConfigurations(){
    }


    /**
     * 开始爬取，迭代次数为depth
     *
     * @param depth 迭代次数
     * @throws Exception 异常
     */
    public void start(int depth) throws Exception {

        LOG.info(this.toString());

        // register conf to all plugins
        // except [fetcher, generatorFilter]
        ConfigurationUtils.setTo(this, dbManager, executor, nextFilter);
        registerOtherConfigurations();


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

        if (!seeds.isEmpty()) {
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
            fetcher = new Fetcher();
            //register fetcher conf
            ConfigurationUtils.setTo(this, fetcher);

            fetcher.setDBManager(dbManager);
            fetcher.setExecutor(executor);
            fetcher.setNextFilter(nextFilter);
            fetcher.setThreads(threads);
            int totalGenerate = fetcher.fetchAll(generatorFilter);

            long endTime = System.currentTimeMillis();
            long costTime = (endTime - startTime) / 1000;

            LOG.info("depth " + (i + 1) + " finish: \n\ttotal urls:\t" + totalGenerate + "\n\ttotal time:\t" + costTime + " seconds");
            if (totalGenerate == 0) {
                break;
            }

        }
        dbManager.close();
        afterStop();
    }

    public void afterStop(){

    }

    /**
     * 停止爬虫
     */
    public void stop() {
        status = STOPED;
        fetcher.stop();
    }

    /**
     * 添加种子任务
     *
     * @param datum 种子任务
     * @param force 如果添加的种子是已爬取的任务，当force为true时，会强制注入种子，当force为false时，会忽略该种子
     */
    public void addSeed(CrawlDatum datum, boolean force) {
        addSeedAndReturn(datum,force);
    }

    /**
         * 等同于 addSeed(datum, false)
         *
         * @param datum 种子任务
         */
    public void addSeed(CrawlDatum datum) {
        addSeedAndReturn(datum);
    }



    /**
     * 添加种子集合
     *
     * @param datums 种子集合
     * @param force 如果添加的种子是已爬取的任务，当force为true时，会强制注入种子，当force为false时，会忽略该种子
     */
    public void addSeed(CrawlDatums datums, boolean force) {
        addSeedAndReturn(datums,force);
    }

    /**
     * 等同于 addSeed(datums,false)
     *
     * @param datums 种子任务集合
     */
    public void addSeed(CrawlDatums datums) {
        addSeedAndReturn(datums);
    }

    /**
     * 与addSeed(CrawlDatums datums, boolean force) 类似
     *
     * @param links 种子URL集合
     * @param type 种子的type标识信息
     * @param force 是否强制注入
     */
    public void addSeed(Iterable<String> links, String type, boolean force) {
        addSeedAndReturn(links, force).type(type);
    }

    /**
     * 与addSeed(CrawlDatums datums, boolean force) 类似
     *
     * @param links 种子URL集合
     * @param force 是否强制注入
     */
    public void addSeed(Iterable<String> links, boolean force) {
        addSeedAndReturn(links,force);
    }


    /**
     * 与addSeed(CrawlDatums datums)类似
     *
     * @param links 种子URL集合
     */
    public void addSeed(Iterable<String> links) {
        addSeedAndReturn(links);
    }

    /**
     * 与addSeed(CrawlDatums datums)类似
     *
     * @param links 种子URL集合
     * @param type 种子的type标识信息
     */
    public void addSeed(Iterable<String> links, String type) {
        addSeedAndReturn(links).type(type);
    }


    /**
     * 与addSeed(CrawlDatum datum, boolean force)类似
     *
     * @param url 种子URL
     * @param type 种子的type标识信息
     * @param force 是否强制注入
     */
    public void addSeed(String url, String type, boolean force) {
        addSeedAndReturn(url,force).type(type);
    }

    /**
     * 与addSeed(CrawlDatum datum, boolean force)类似
     *
     * @param url 种子URL
     * @param force 是否强制注入
     */
    public void addSeed(String url, boolean force) {
        addSeedAndReturn(url,force);
    }




    /**
     * 与addSeed(CrawlDatum datum)类似
     *
     * @param type 种子的type标识信息
     * @param url 种子URL
     */
    public void addSeed(String url, String type) {
        addSeedAndReturn(url).type(type);
    }

    /**
     * 与addSeed(CrawlDatum datum)类似
     *
     * @param url 种子URL
     */
    public void addSeed(String url) {
        addSeedAndReturn(url);
    }

    public CrawlDatum addSeedAndReturn(CrawlDatum datum, boolean force) {
        if (force) {
            forcedSeeds.add(datum);
        } else {
            seeds.add(datum);
        }
        return datum;
    }

    public CrawlDatum addSeedAndReturn(CrawlDatum datum) {
        return addSeedAndReturn(datum,false);
    }

    public CrawlDatum addSeedAndReturn(String url, boolean force) {
        CrawlDatum datum = new CrawlDatum(url);
        return addSeedAndReturn(datum,force);
    }

    public CrawlDatum addSeedAndReturn(String url) {
        return addSeedAndReturn(url,false);
    }

    public CrawlDatums addSeedAndReturn(Iterable<String> links, boolean force) {
        CrawlDatums datums = new CrawlDatums(links);
        return addSeedAndReturn(datums,force);
    }

    public CrawlDatums addSeedAndReturn(Iterable<String> links) {
        return addSeedAndReturn(links,false);
    }

    public CrawlDatums addSeedAndReturn(CrawlDatums datums, boolean force) {
        if (force) {
            forcedSeeds.add(datums);
        } else {
            seeds.add(datums);
        }
        return datums;
    }

    public CrawlDatums addSeedAndReturn(CrawlDatums datums) {
        return addSeedAndReturn(datums,false);
    }


    public GeneratorFilter getGeneratorFilter() {
        return generatorFilter;
    }

    public void setGeneratorFilter(GeneratorFilter generatorFilter) {
        this.generatorFilter = generatorFilter;
    }

    /**
     * 返回是否断点爬取
     *
     * @return 是否断点爬取
     */
    public boolean isResumable() {
        return resumable;
    }

    /**
     * 设置是否断点爬取
     *
     * @param resumable 是否断点爬取
     */
    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    /**
     * 返回线程数
     *
     * @return 线程数
     */
    public int getThreads() {
        return threads;
    }

    /**
     * 设置线程数
     *
     * @param threads 线程数
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getMaxExecuteCount() {
        return maxExecuteCount;
    }

    /**
     * 设置每个爬取任务的最大执行次数，爬取或解析失败都会导致执行失败。 当一个任务执行失败时，爬虫会在后面的迭代中重新执行该任务，
     * 当该任务执行失败的次数超过最大执行次数时，任务生成器会忽略该任务
     *
     * @param maxExecuteCount 每个爬取任务的最大执行次数
     */
    public void setMaxExecuteCount(int maxExecuteCount) {
        this.maxExecuteCount = maxExecuteCount;
    }

    /**
     * 获取每个爬取任务的最大执行次数
     *
     * @return 每个爬取任务的最大执行次数
     */
    public Executor getExecutor() {
        return executor;
    }

//    /**
//     * 设置执行器
//     *
//     * @param executor 执行器
//     */
//    public void setExecutor(Executor executor) {
//        this.executor = executor;
//    }

    /**
     * 返回每次迭代爬取的网页数量上限
     *
     * @return 每次迭代爬取的网页数量上限
     */


    /**
     * 设置每次迭代爬取的网页数量上限
     *
     * @param topN 每次迭代爬取的网页数量上限
     */


    /**
     * 获取执行间隔
     *
     * @return 执行间隔
     */

    /**
     * 设置执行间隔
     *
     * @param executeInterval 执行间隔
     */

    /**
     * 返回任务管理器
     *
     * @return 任务管理器
     */
    public DBManager getDBManager() {
        return dbManager;
    }

    /**
     * 设置任务管理器
     *
     * @param dbManager 任务管理器
     */
    public void setDBManager(DBManager dbManager) {
        this.dbManager = dbManager;
    }

    public NextFilter getNextFilter() {
        return nextFilter;
    }

    public void setNextFilter(NextFilter nextFilter) {
        this.nextFilter = nextFilter;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Crawler Details:\n")
                .append("\tCrawler: ").append(getClass()).append("\n")
                .append("\tExecutor: ").append(executor.getClass()).append("\n")
                .append("\tDBManager: ").append(dbManager.getClass()).append("\n")
                .append("\tNextFilter: ");
        if (nextFilter == null) {
            sb.append("null");
        } else {
            sb.append(nextFilter.getClass());
        }
        return sb.toString();
    }

}
