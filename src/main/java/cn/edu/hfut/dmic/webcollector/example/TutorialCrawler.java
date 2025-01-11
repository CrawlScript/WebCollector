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

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;
//import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;



/**
 * WebCollector 2.x version tutorial (2.20 and above)

 * 2.x version features:
 * 1) Custom traversal strategy, capable of handling more complex traversal tasks such as pagination and AJAX.
 * 2) Each URL can have additional MetaData, supporting tasks like depth fetching, anchor text extraction, referenced page retrieval, POST parameter transmission, and incremental updates.
 * 3) Plugin mechanism with two built-in plugins.
 * 4) A memory-based plugin (RamCrawler) suitable for one-time tasks like real-time search engine crawling.
 * 5) A Berkeley DB-based plugin (BreadthCrawler) suitable for long-term and large-scale tasks with resume capability.
 * 6) Selenium integration for JavaScript-generated content extraction.
 * 7) Flexible HTTP request customization with multi-proxy random switching, supporting simulated login.
 * 8) Uses slf4j as a logging facade compatible with multiple logging frameworks.

 * 示例教程（2.20以上）
 * 2.x版本特性：
 * 1）自定义遍历策略，可完成更为复杂的遍历业务，例如分页、AJAX。
 * 2）可以为每个URL设置附加信息(MetaData)，可用于深度抓取、锚文本获取、引用页面获取、POST参数传递、增量更新等。
 * 3）使用插件机制，内置两套插件。
 * 4）内置基于内存的插件(RamCrawler)，适合一次性爬取，例如实时爬取搜索引擎。
 * 5）内置基于Berkeley DB（BreadthCrawler)的插件，适用于长期和大规模任务，并具有断点续爬功能。
 * 6）集成selenium，可用于提取JavaScript生成的信息。
 * 7）支持自定义HTTP请求，并内置多代理随机切换功能，可通过自定义请求实现模拟登录。
 * 8）使用slf4j作为日志门面，可对接多种日志。

 * Examples can be found in the package: cn.edu.hfut.dmic.webcollector.example.
 * 示例可在cn.edu.hfut.dmic.webcollector.example包中找到。

 * @author hu
 */
public class TutorialCrawler extends BreadthCrawler {

    /*
        This example uses regular expressions to control the crawler's traversal.
        For another common traversal method, refer to DemoTypeCrawler.

        该例子利用正则控制爬虫的遍历。
        另一种常用遍历方法可参考DemoTypeCrawler。
    */
    public TutorialCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        
        addSeed("https://blog.csdn.net/");
        addRegex("https://blog.csdn.net/.*/article/details/.*");
        addRegex("-.*#.*");

        // If you need to crawl images, set this to true and add the regex for images.
        //需要抓取图片时设置为true，并加入图片的正则规则
//        setParseImg(true);

        // Set the crawling interval for each thread (in milliseconds).
        //设置每个线程的抓取间隔（毫秒）
//        setExecuteInterval(1000);
        getConf().setExecuteInterval(1000);

        // Set the number of threads.
        //设置线程数
        setThreads(30);
    }


    /*
        Tasks can be added to `next` for further crawling. Tasks can be either a URL or a CrawlDatum.
        The crawler avoids duplicate tasks based on the CrawlDatum's key (not the URL).
        Therefore, if you want to repeatedly crawl a specific URL, simply set the CrawlDatum's key to a value that has not previously existed.

        For incremental crawling, you can use a combination of crawl time and URL as the key.

        The new version allows extracting webpage information using `page.select(cssSelector)` directly,
        which is equivalent to `page.getDoc().select(cssSelector)`.
        The `page.getDoc()` method returns a Jsoup `Document` object.
        For more details, please refer to the Jsoup documentation.

        可以往next中添加希望后续爬取的任务，任务可以是URL或者CrawlDatum。
        爬虫从2.20版开始，基于CrawlDatum的key去重，而不是URL。
        因此如果希望重复爬取某个URL，只要将CrawlDatum的key设置为一个历史中不存在的值即可。

        例如增量爬取，可以使用爬取时间+URL作为key。

        新版本中，可以直接通过 `page.select(css选择器)` 方法来抽取网页中的信息，
        等价于 `page.getDoc().select(css选择器)` 方法。
        `page.getDoc()` 获取到的是Jsoup中的 Document 对象，
        细节请参考Jsoup教程。
    */
    @Override
    public void visit(Page page, CrawlDatums next) {
        if (page.matchUrl("https://blog.csdn.net/.*/article/details/.*")) {
            String title = page.select("h1.title-article").first().text();
            String author = page.select("a#uid").first().text();
            System.out.println("title:" + title + "\tauthor:" + author);
        }
    }


    public static void main(String[] args) throws Exception {
        TutorialCrawler crawler = new TutorialCrawler("crawl", true);
//        crawler.setResumable(true);
        crawler.start(3);
    }

}
