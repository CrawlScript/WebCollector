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


import cn.edu.hfut.dmic.webcollector.fetcher.FSFetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.generator.FSInjector;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.generator.FSGenerator;
import cn.edu.hfut.dmic.webcollector.generator.filter.IntervalFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.URLRegexFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.UniqueFilter;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.output.FileSystemOutput;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.util.ArrayList;



/**
 * 广度遍历爬虫
 * 
 * @author hu
 */
public class BreadthCrawler{
    

    private String crawlPath = "crawl";
    private String root = "data";
    private String cookie = null;
    private String useragent = "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:26.0) Gecko/20100101 Firefox/26.0";

    private int threads=10;
    private boolean resumable=false;
    private boolean isContentStored=true;
    private Proxy proxy=null;
    private ConnectionConfig conconfig = null;
    

    private ArrayList<String> regexs = new ArrayList<String>();
    private ArrayList<String> seeds = new ArrayList<String>();

    public final static int RUNNING=1;
    public final static int STOPED=2;
    private int status;
    private Fetcher fetcher;

    /**
     * 添加一个种子url
     * @param seed 种子url
     */
    public void addSeed(String seed) {
        seeds.add(seed);
    }
    
    /**
     * 添加一个正则过滤规则
     * @param regex 正则过滤规则
     */
    public void addRegex(String regex) {
        regexs.add(regex);
    }


    /**
     * 对每个成功爬取的页面（文件）进行的操作，可以通过Override这个方法来完成用户对这些页面的自定义处理
     * @param page 爬取的页面（文件）
     */
    protected void visit(Page page) {
        FileSystemOutput fsoutput = new FileSystemOutput(root);
        LogUtils.getLogger().info("visit "+page.getUrl());
        fsoutput.output(page);
    }
    
    /**
     * 对每个失败爬取的页面（文件）进行的操作，可以通过Override这个方法来完成用户对这些页面的自定义处理
     * @param page 爬取的页面（文件）
     */
    protected void failed(Page page){
       
    }
    
    /**
     * 启动爬虫
     * @param depth 广度遍历的深度
     * @throws IOException
     */
    public void start(int depth) throws Exception {
        if (!resumable) {
            File crawlDir=new File(crawlPath);
            if(crawlDir.exists()){
                FileUtils.deleteDir(crawlDir);
            }
                        
            if (seeds.isEmpty()) {
                LogUtils.getLogger().info("error:Please add at least one seed");
                return;
            }
           
        }
        if (regexs.isEmpty()) {
                LogUtils.getLogger().info("error:Please add at least one regex rule");
                return;
        }
        inject();
        status=RUNNING;
        for (int i = 0; i < depth; i++) {
           if(status==STOPED){
               break;
            }
            LogUtils.getLogger().info("starting depth "+(i+1));
            Generator generator=createGenerator();
            fetcher=createFecther();
            fetcher.fetchAll(generator);
        }
    }
    
    

    /**
     * 停止爬虫
     * @throws IOException
     */
    public void stop() throws IOException{
       fetcher.stop();
       status=STOPED;
    }
    
    private void inject() throws Exception {
        
        Injector injector = createInjector();
        injector.inject(seeds,resumable);
        
    }

    class CommonConnectionConfig implements ConnectionConfig{
        @Override
            public void config(HttpURLConnection con) {               
                con.setRequestProperty("User-Agent", useragent);
                if (cookie != null) {
                    con.setRequestProperty("Cookie", cookie);
                }
            }
    }
    
    /**
     * 生成处理抓取消息的Handler，默认通过BreadthCrawler的visit方法来处理成功抓取的页面，
     * 通过failed方法来处理失败抓取的页面
     * @return 处理抓取消息的Handler
     */
    protected Handler createFetcherHandler(){
        Handler fetch_handler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                switch(msg.what){
                    case Fetcher.FETCH_SUCCESS:
                        
                        visit(page);
                        break;
                    case Fetcher.FETCH_FAILED:
                        failed(page);
                        break;
                    default:
                        break;
                       
                }
            }
        };
        return fetch_handler;
    }
    
    /**
     * 生成Fetcher(抓取器)的方法，可以通过Override这个方法来完成自定义Fetcher
     * @return 生成的抓取器
     */
    protected Fetcher createFecther(){
  
        Fetcher fetcher=new FSFetcher(crawlPath);
        fetcher.setProxy(proxy);
        fetcher.setIsContentStored(isContentStored);
        fetcher.setHandler(createFetcherHandler());
        conconfig = new CommonConnectionConfig();
        
        fetcher.setThreads(threads);
        fetcher.setConconfig(conconfig);
        return fetcher;
    }
    
    /**
     * 生成Generator(抓取任务生成器）的方法，可以通过Override这个方法来完成自定义Generator
     * @return 生成的抓取任务生成器
     */
    protected Generator createGenerator(){

        Generator generator = new FSGenerator(crawlPath);
        generator=new UniqueFilter(new IntervalFilter(new URLRegexFilter(generator, regexs)));
  
        return generator;
    }
    
    protected Injector createInjector(){
        return new FSInjector(crawlPath);
    }
   
   

    /**
     * 返回User-Agent
     * @return User-Agent
     */
    public String getUseragent() {
        return useragent;
    }

    /**
     * 设置User-Agent
     * @param useragent
     */
    public void setUseragent(String useragent) {
        this.useragent = useragent;
    }

    /**
     * 返回爬虫的线程数
     * @return 爬虫的线程数
     */
    public int getThreads() {
        return threads;
    }

    /**
     * 设置爬虫的线程数
     * @param threads 线程数
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * 返回存储爬虫爬取信息的文件夹路径
     * @return 存储爬虫爬取信息的文件夹路径
     */
    public String getCrawlPath() {
        return crawlPath;
    }

    /**
     * 设置存储爬虫爬取信息的文件夹路径
     * @param crawlPath 存储爬虫爬取信息的文件夹路径
     */
    public void setCrawlPath(String crawlPath) {
        this.crawlPath = crawlPath;
    }

    /**
     * 返回Cookie
     * @return Cookie
     */
    public String getCookie() {
        return cookie;
    }

    /**
     * 设置http请求的cookie
     * @param cookie Cookie
     */
    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    /**
     * 如果使用默认的visit，返回存储网页文件的路径
     * @return 如果使用默认的visit，存储网页文件的路径
     */
    @Deprecated
    public String getRoot() {
        return root;
    }

    /**
     * 如果使用默认的visit,设置存储网页文件的路径
     * @param root 如果使用默认的visit,存储网页文件的路径
     */
    @Deprecated
    public void setRoot(String root) {
        this.root = root;
    }

    /**
     * 返回爬虫是否为断点爬取模式
     * @return 爬虫是否为断点爬取模式
     */
    public boolean getResumable() {
        return resumable;
    }

    /**
     * 设置爬虫是否为可断点模式
     * @param resumable 爬虫是否为可断点模式
     */
    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    /**
     * 返回http连接配置对象
     * @return http连接配置对象
     */
    public ConnectionConfig getConconfig() {
        return conconfig;
    }

    /**
     * 设置http连接配置对象
     * @param conconfig http连接配置对象
     */
    public void setConconfig(ConnectionConfig conconfig) {
        this.conconfig = conconfig;
    }

    /**
     * 返回是否存储网页/文件的内容
     * @return 是否存储网页/文件的内容
     */
    public boolean isIsContentStored() {
        return isContentStored;
    }

    /**
     * 设置是否存储网页／文件的内容
     * @param isContentStored 是否存储网页/文件的内容
     */
    public void setIsContentStored(boolean isContentStored) {
        this.isContentStored = isContentStored;
    }

    /**
     * 返回代理
     * @return 代理
     */
    public Proxy getProxy() {
        return proxy;
    }

    /**
     * 设置代理
     * @param proxy 代理
     */
    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }

    /**
     * 返回正则过滤规则列表
     * @return 正则过滤规则列表
     */
    public ArrayList<String> getRegexs() {
        return regexs;
    }

    /**
     * 设置正则过滤规则列表
     * @param regexs 正则过滤规则列表
     */
    public void setRegexs(ArrayList<String> regexs) {
        this.regexs = regexs;
    }

    /**
     * 返回种子url列表
     * @return 种子url列表
     */
    public ArrayList<String> getSeeds() {
        return seeds;
    }

    /**
     * 设置种子url列表
     * @param seeds 种子url列表
     */
    public void setSeeds(ArrayList<String> seeds) {
        this.seeds = seeds;
    }

    
    public static void main(String[] args) throws Exception {
        
        
        String crawl_path = "/home/hu/data/crawl_hfut1";
        String root = "/home/hu/data/hfut1";       
        //LogUtils.setLogger(LogUtils.createCommonLogger("hfut"));
        //Config.topN=100;
        BreadthCrawler crawler=new BreadthCrawler(){
            @Override
            public void visit(Page page){
            System.out.println(page.getUrl()+" "+page.getResponse().getCode());
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
       
        crawler.setResumable(false);      
        crawler.start(3);
        
        
    }
    
    
    
    
}
