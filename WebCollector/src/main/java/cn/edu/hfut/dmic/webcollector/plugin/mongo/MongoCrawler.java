/*
 * Copyright (C) 2015 hu
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
package cn.edu.hfut.dmic.webcollector.plugin.mongo;

import cn.edu.hfut.dmic.webcollector.crawler.AutoParseCrawler;
import com.mongodb.MongoClient;

/**
 * BreadthCrawler是基于Berkeley DB的插件,于2.20版重新设计
 * BreadthCrawler可以设置正则规律，让遍历器自动根据URL的正则遍历网站，可以关闭这个功能，自定义遍历
 * 如果autoParse设置为true，遍历器会自动解析页面中符合正则的链接，加入后续爬取任务，否则不自动解析链接。
 * 注意，爬虫会保证爬取任务的唯一性，也就是会自动根据CrawlDatum的key进行去重，默认情况下key就是URL，
 * 所以用户在编写爬虫时完全不必考虑生成重复URL的问题。
 * 断点爬取中，爬虫仍然会保证爬取任务的唯一性。
 *
 * @author hu
 */
public abstract class MongoCrawler extends AutoParseCrawler {

      public MongoCrawler(String crawlID,MongoClient client,boolean autoParse) {
        super(autoParse);
        this.dbManager=new MongoDBManager(crawlID, client);
    }
    
}
