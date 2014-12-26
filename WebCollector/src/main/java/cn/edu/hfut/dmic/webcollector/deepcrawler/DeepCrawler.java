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

import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.FSDbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.FSGenerator;
import cn.edu.hfut.dmic.webcollector.generator.FSInjector;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.generator.filter.IntervalFilter;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;



/**
 * DeepCrawler是为深网爬取设置的一个爬虫
 * 虽然本质还是广度遍历，但是用户可以通过自定义visitor,
 * 来指定访问每个页面后，生成的新链接。也就是说，可以自定义广度遍历的树中，每个节点
 * 的孩子节点。通过自定义Visitor，可以完成一些复杂的爬取。比如翻页爬取。也可以对Ajax信息
 * 例如一些获取JSON的API,进行爬取。
 * @author hu
 */
public abstract class DeepCrawler extends CommonDCrawler{
    
    
        
        
    

    private String crawlPath = "crawl";
    private String root = "data";
 

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
        //generator=new URLRegexFilter(new IntervalFilter(generator), getRegexRule());
        generator=new IntervalFilter(generator);
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
    
    

   

    
}
