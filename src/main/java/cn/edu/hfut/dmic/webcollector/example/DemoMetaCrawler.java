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
package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.ram.RamCrawler;


/**
 * WebCollector 2.x版本的tutorial(2.20以上) 
 * 2.x版本特性：
 * 1）自定义遍历策略，可完成更为复杂的遍历业务，例如分页、AJAX
 * 2）可以为每个URL设置附加信息(MetaData)，利用附加信息可以完成很多复杂业务，例如深度获取、锚文本获取、引用页面获取、POST参数传递、增量更新等。
 * 3）使用插件机制，WebCollector内置两套插件。
 * 4）内置一套基于内存的插件（RamCrawler)，不依赖文件系统或数据库，适合一次性爬取，例如实时爬取搜索引擎。
 * 5）内置一套基于Berkeley DB（BreadthCrawler)的插件：适合处理长期和大量级的任务，并具有断点爬取功能，不会因为宕机、关闭导致数据丢失。 
 * 6）集成selenium，可以对javascript生成信息进行抽取
 * 7）可轻松自定义http请求，并内置多代理随机切换功能。 可通过定义http请求实现模拟登录。 
 * 8）使用slf4j作为日志门面，可对接多种日志
 *
 * 可在cn.edu.hfut.dmic.webcollector.example包中找到例子(Demo)
 *
 * @author hu
 */
public class DemoMetaCrawler extends RamCrawler {

    /*
        实际使用时建议按照DemoTypeCrawler的方式操作，该教程目的为阐述meta的原理
    
        可以往next中添加希望后续爬取的任务，任务可以是URL或者CrawlDatum
        爬虫不会重复爬取任务，从2.20版之后，爬虫根据CrawlDatum的key去重，而不是URL
        因此如果希望重复爬取某个URL，只要将CrawlDatum的key设置为一个历史中不存在的值即可
        例如增量爬取，可以使用 爬取时间+URL作为key。
    
        新版本中，可以直接通过 page.select(css选择器)方法来抽取网页中的信息，等价于
        page.getDoc().select(css选择器)方法，page.getDoc()获取到的是Jsoup中的
        Document对象，细节请参考Jsoup教程
    
        该Demo爬虫需要应对豆瓣图书的三种页面：
        1）标签页（taglist，包含图书列表页的入口链接）
        2）列表页（booklist，包含图书详情页的入口链接）
        3）图书详情页（content）
     */
    @Override
    public void visit(Page page, CrawlDatums next) {

        String type=page.meta("type");
        //如果是列表页，抽取内容页链接，放入后续任务中
        if(type.equals("taglist")){
            //可以确定抽取到的链接都指向内容页
            //因此为这些链接添加附加信息（meta）：type=content
            next.addAndReturn(page.links("table.tagCol td>a")).meta("type", "booklist");
        }else if(type.equals("booklist")){
            next.addAndReturn(page.links("div.info>h2>a")).meta("type", "content");
        }else if(type.equals("content")){
            //处理内容页，抽取书名和豆瓣评分
            String title=page.select("h1>span").first().text();
            String score=page.select("strong.ll.rating_num").first().text();
            System.out.println("title:"+title+"\tscore:"+score);
        }

    }

    public static void main(String[] args) throws Exception {
        DemoMetaCrawler crawler = new DemoMetaCrawler();
        //meta是CrawlDatum的附加信息，爬虫内核并不使用meta信息
        //在解析页面时，往往需要知道当前页面的类型（例如是列表页还是内容页）或一些附加信息（例如页号）
        //然而根据当前页面的信息（内容和URL）并不一定能够轻易得到这些信息
        //例如当在解析页面 https://book.douban.com/tag/ 时，需要知道该页是目录页还是内容页
        //虽然用正则可以解决这个问题，但是较为麻烦
        //当我们将一个新链接（CrawlDatum）提交给爬虫时，链接指向页面的类型有时是确定的（例如在很多任务中，种子页面就是列表页）
        //如果在提交CrawlDatum时，直接将链接的类型信息（type）存放到meta中，那么在解析页面时，
        //只需取出链接（CrawlDatum）中的类型信息（type）即可知道当前页面类型           
        CrawlDatum seed=new CrawlDatum("https://book.douban.com/tag/").meta("type", "taglist");
        crawler.addSeed(seed);


        /*可以设置每个线程visit的间隔，这里是毫秒*/
        //crawler.setVisitInterval(1000);
        /*可以设置http请求重试的间隔，这里是毫秒*/
        //crawler.setRetryInterval(1000);
        crawler.setThreads(30);
        crawler.start(3);
    }

}