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
import cn.edu.hfut.dmic.webcollector.util.LogUtils;


/**
 * 基于文件系统的广度遍历爬虫
 * @author hu
 */
public class BreadthCrawler extends CommonCrawler{
    
    private String crawlPath = "crawl";
    private String root = "data";
 

    
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

    

    @Override
    public Injector createInjector() {
        return new FSInjector(crawlPath);
    }

    
    @Override
    public Generator createGenerator() {

        Generator generator = new FSGenerator(crawlPath);
        generator = new UniqueFilter(new IntervalFilter(new URLRegexFilter(generator, getRegexs())));
        return generator;
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

    

    

   
/*
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
    */

}
