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

import cn.edu.hfut.dmic.webcollector.fetcher.BasicFetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.FSDbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.FSGenerator;
import cn.edu.hfut.dmic.webcollector.generator.FSInjector;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.generator.filter.IntervalFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.URLRegexFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.UniqueFilter;

import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.output.FileSystemOutput;
import cn.edu.hfut.dmic.webcollector.util.CommonConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;

import cn.edu.hfut.dmic.webcollector.util.LogUtils;

import java.net.Proxy;


/**
 * 广度遍历爬虫
 *
 * @author hu
 */
public class BreadthCrawler extends BasicCrawler {

    private String crawlPath = "crawl";
    private String root = "data";
    private String cookie = null;
    private String useragent = "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:26.0) Gecko/20100101 Firefox/26.0";

    private boolean isContentStored = true;
    private Proxy proxy = null;
    private ConnectionConfig conconfig = null;

    /**
     * 对每个成功爬取的页面（文件）进行的操作，可以通过Override这个方法来完成用户对这些页面的自定义处理
     *
     * @param page 爬取的页面（文件）
     */
    @Override
    public void visit(Page page) {
        FileSystemOutput fsoutput = new FileSystemOutput(root);
        LogUtils.getLogger().info("visit " + page.getUrl());
        fsoutput.output(page);
    }

    @Override
    public DbUpdater createDbUpdater() {
        return new FSDbUpdater(crawlPath);
    }
    
    /**
     * 生成Fetcher(抓取器)的方法，可以通过Override这个方法来完成自定义Fetcher
     *
     * @return 生成的抓取器
     */
    @Override
    public Fetcher createFetcher() {

        BasicFetcher fetcher = new BasicFetcher();
        fetcher.setNeedUpdateDb(true);
        fetcher.setProxy(proxy);
        fetcher.setIsContentStored(isContentStored);
        fetcher.setHandler(createFetcherHandler());
        conconfig = new CommonConnectionConfig(useragent, cookie);
        fetcher.setThreads(getThreads());
        fetcher.setConconfig(conconfig);
        return fetcher;
    }
    
     @Override
    public Injector createInjector() {
        return new FSInjector(crawlPath);
    }

    /**
     * 生成Generator(抓取任务生成器）的方法，可以通过Override这个方法来完成自定义Generator
     *
     * @return 生成的抓取任务生成器
     */
    @Override
    public Generator createGenerator() {

        Generator generator = new FSGenerator(crawlPath);
        generator = new UniqueFilter(new IntervalFilter(new URLRegexFilter(generator, getRegexs())));

        return generator;
    }

   

    /**
     * 返回User-Agent
     *
     * @return User-Agent
     */
    public String getUseragent() {
        return useragent;
    }

    /**
     * 设置User-Agent
     *
     * @param useragent
     */
    public void setUseragent(String useragent) {
        this.useragent = useragent;
    }

    /**
     * 返回存储爬虫爬取信息的文件夹路径
     *
     * @return 存储爬虫爬取信息的文件夹路径
     */
    public String getCrawlPath() {
        return crawlPath;
    }

    /**
     * 设置存储爬虫爬取信息的文件夹路径
     *
     * @param crawlPath 存储爬虫爬取信息的文件夹路径
     */
    public void setCrawlPath(String crawlPath) {
        this.crawlPath = crawlPath;
    }

    /**
     * 返回Cookie
     *
     * @return Cookie
     */
    public String getCookie() {
        return cookie;
    }

    /**
     * 设置http请求的cookie
     *
     * @param cookie Cookie
     */
    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    /**
     * 如果使用默认的visit，返回存储网页文件的路径
     *
     * @return 如果使用默认的visit，存储网页文件的路径
     */
    @Deprecated
    public String getRoot() {
        return root;
    }

    /**
     * 如果使用默认的visit,设置存储网页文件的路径
     *
     * @param root 如果使用默认的visit,存储网页文件的路径
     */
    @Deprecated
    public void setRoot(String root) {
        this.root = root;
    }

    /**
     * 返回http连接配置对象
     *
     * @return http连接配置对象
     */
    public ConnectionConfig getConconfig() {
        return conconfig;
    }

    /**
     * 设置http连接配置对象
     *
     * @param conconfig http连接配置对象
     */
    public void setConconfig(ConnectionConfig conconfig) {
        this.conconfig = conconfig;
    }

    /**
     * 返回是否存储网页/文件的内容
     *
     * @return 是否存储网页/文件的内容
     */
    public boolean getIsContentStored() {
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
     * 返回代理
     *
     * @return 代理
     */
    public Proxy getProxy() {
        return proxy;
    }

    /**
     * 设置代理
     *
     * @param proxy 代理
     */
    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }

    public static void main(String[] args) throws Exception {

        String crawl_path = "/home/hu/data/crawl_hfut1";
        String root = "/home/hu/data/hfut1";
        //LogUtils.setLogger(LogUtils.createCommonLogger("hfut"));
        //Config.topN=100;
        BreadthCrawler crawler = new BreadthCrawler() {
            @Override
            public void visit(Page page) {
                System.out.println(page.getUrl() + " " + page.getResponse().getCode());
                System.out.println(page.getDoc().title());

            }
        };

        crawler.addSeed("http://news.hfut.edu.cn/");
        crawler.addRegex("http://news.hfut.edu.cn/.*");
        crawler.addRegex("-.*#.*");
        crawler.addRegex("-.*png.*");
        crawler.addRegex("-.*jpg.*");
        crawler.addRegex("-.*gif.*");
        crawler.addRegex("-.*js.*");
        crawler.addRegex("-.*css.*");

        //crawler.addRegex(".*");
        crawler.setRoot(root);
        crawler.setCrawlPath(crawl_path);

        crawler.setResumable(true);
        crawler.start(4);

    }

    

    

}
