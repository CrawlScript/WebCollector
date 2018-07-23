# WebCollector
WebCollector is an open source web crawler framework based on Java.It provides
  some simple interfaces for crawling the Web,you can setup a
  multi-threaded web crawler in less than 5 minutes.

## HomePage
[https://github.com/CrawlScript/WebCollector](https://github.com/CrawlScript/WebCollector)


## Installation


### Using Maven

```xml
<dependency>
    <groupId>cn.edu.hfut.dmic.webcollector</groupId>
    <artifactId>WebCollector</artifactId>
    <version>2.73-alpha</version>
</dependency>
```


### Without Maven
WebCollector jars are available on the [HomePage](https://github.com/CrawlScript/WebCollector).

+ __webcollector-version-bin.zip__ contains core jars.



## Example Index

Annotation versions are named with `DemoAnnotatedxxxxxx.java`.

### Basic

+ [DemoAutoNewsCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAutoNewsCrawler.java) | [DemoAnnotatedAutoNewsCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedAutoNewsCrawler.java)
+ [DemoManualNewsCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoManualNewsCrawler.java) | [DemoAnnotatedManualNewsCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedManualNewsCrawler.java)
+ [DemoExceptionCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoExceptionCrawler.java)

### CrawlDatum and MetaData

+ [DemoMetaCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoMetaCrawler.java)
+ [DemoAnnotatedMatchTypeCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedMatchTypeCrawler.java)
+ [DemoAnnotatedDepthCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedDepthCrawler.java)
+ [DemoBingCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoBingCrawler.java) | [DemoAnnotatedBingCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedBingCrawler.java)
+ [DemoAnnotatedDepthCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedDepthCrawler.java)

### Http Request and Javascript

+ [DemoCookieCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoCookieCrawler.java)
+ [DemoRedirectCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoRedirectCrawler.java)  | [DemoAnnotatedRedirectCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAnnotatedRedirectCrawler.java)
+ [DemoPostCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoPostCrawler.java)
+ [DemoRandomProxyCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoRandomProxyCrawler.java)
+ [AbuyunDynamicProxyRequester.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/AbuyunDynamicProxyRequester.java)
+ [DemoSeleniumCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoSeleniumCrawler.java)

### NextFilter

+ [DemoNextFilter.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoNextFilter.java)
+ [DemoHashSetNextFilter.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoHashSetNextFilter.java)






## Quickstart
Lets crawl some news from github news.This demo prints out the titles and contents extracted from news of github news.

### Automatically Detecting URLs

[DemoAutoNewsCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoAutoNewsCrawler.java):

```java

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;

/**
 * Crawling news from github news
 *
 * @author hu
 */
public class DemoAutoNewsCrawler extends BreadthCrawler {
    /**
     * @param crawlPath crawlPath is the path of the directory which maintains
     *                  information of this crawler
     * @param autoParse if autoParse is true,BreadthCrawler will auto extract
     *                  links which match regex rules from pag
     */
    public DemoAutoNewsCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        /*start pages*/
        this.addSeed("https://blog.github.com/");
        for(int pageIndex = 2; pageIndex <= 5; pageIndex++) {
            String seedUrl = String.format("https://blog.github.com/page/%d/", pageIndex);
            this.addSeed(seedUrl);
        }

        /*fetch url like "https://blog.github.com/2018-07-13-graphql-for-octokit/" */
        this.addRegex("https://blog.github.com/[0-9]{4}-[0-9]{2}-[0-9]{2}-[^/]+/");
        /*do not fetch jpg|png|gif*/
        //this.addRegex("-.*\\.(jpg|png|gif).*");
        /*do not fetch url contains #*/
        //this.addRegex("-.*#.*");

        setThreads(50);
        getConf().setTopN(100);

        //enable resumable mode
        //setResumable(true);
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        String url = page.url();
        /*if page is news page*/
        if (page.matchUrl("https://blog.github.com/[0-9]{4}-[0-9]{2}-[0-9]{2}[^/]+/")) {

            /*extract title and content of news by css selector*/
            String title = page.select("h1[class=lh-condensed]").first().text();
            String content = page.selectText("div.content.markdown-body");

            System.out.println("URL:\n" + url);
            System.out.println("title:\n" + title);
            System.out.println("content:\n" + content);

            /*If you want to add urls to crawl,add them to nextLink*/
            /*WebCollector automatically filters links that have been fetched before*/
            /*If autoParse is true and the link you add to nextLinks does not match the 
              regex rules,the link will also been filtered.*/
            //next.add("http://xxxxxx.com");
        }
    }

    public static void main(String[] args) throws Exception {
        DemoAutoNewsCrawler crawler = new DemoAutoNewsCrawler("crawl", true);
        /*start crawl with depth of 4*/
        crawler.start(4);
    }

}

```


### Manually Detecting URLs


[DemoManualNewsCrawler.java](src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoManualNewsCrawler.java):

```java

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.BreadthCrawler;

/**
 * Crawling news from github news
 *
 * @author hu
 */
public class DemoManualNewsCrawler extends BreadthCrawler {
    /**
     * @param crawlPath crawlPath is the path of the directory which maintains
     *                  information of this crawler
     * @param autoParse if autoParse is true,BreadthCrawler will auto extract
     *                  links which match regex rules from pag
     */
    public DemoManualNewsCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        // add 5 start pages and set their type to "list"
        //"list" is not a reserved word, you can use other string instead
        this.addSeedAndReturn("https://blog.github.com/").type("list");
        for(int pageIndex = 2; pageIndex <= 5; pageIndex++) {
            String seedUrl = String.format("https://blog.github.com/page/%d/", pageIndex);
            this.addSeed(seedUrl, "list");
        }

        setThreads(50);
        getConf().setTopN(100);

        //enable resumable mode
        //setResumable(true);
    }

    @Override
    public void visit(Page page, CrawlDatums next) {
        String url = page.url();

        if (page.matchType("list")) {
            /*if type is "list"*/
            /*detect content page by css selector and mark their types as "content"*/
            next.add(page.links("h1.lh-condensed>a")).type("content");
        }else if(page.matchType("content")) {
            /*if type is "content"*/
            /*extract title and content of news by css selector*/
            String title = page.select("h1[class=lh-condensed]").first().text();
            String content = page.selectText("div.content.markdown-body");

            //read title_prefix and content_length_limit from configuration
            title = getConf().getString("title_prefix") + title;
            content = content.substring(0, getConf().getInteger("content_length_limit"));

            System.out.println("URL:\n" + url);
            System.out.println("title:\n" + title);
            System.out.println("content:\n" + content);
        }

    }

    public static void main(String[] args) throws Exception {
        DemoManualNewsCrawler crawler = new DemoManualNewsCrawler("crawl", false);

        crawler.getConf().setExecuteInterval(5000);

        crawler.getConf().set("title_prefix","PREFIX_");
        crawler.getConf().set("content_length_limit", 20);

        /*start crawl with depth of 4*/
        crawler.start(4);
    }

}

```

## CrawlDatum

CrawlDatum is an important data structure in WebCollector, which corresponds to url of webpages. Both crawled urls and detected urls are maintained as CrawlDatums.

There are some differences between CrawlDatum and url:

+ A CrawlDatum contains a key and a url. The key is the url by default. You can set the key manually by CrawlDatum.key("xxxxx") so that CrawlDatums with the same url may have different keys. This is very useful in some tasks like crawling data by api, which often request different data by the same url with different post parameters.
+ A CrawlDatum may contain metadata, which could maintain some information besides the url. 

## Manually Detecting URLs

In both `void visit(Page page, CrawlDatums next)` and `void execute(Page page, CrawlDatums next)`, the second parameter `CrawlDatum next` is a container which you should put the detected URLs in:

```java
//add one detected URL
next.add("detected URL");
//add one detected URL and set its type
next.add("detected URL", "type");
//add one detected URL
next.add(new CrawlDatum("detected URL"));
//add detected URLs
next.add("detected URL list");
//add detected URLs
next.add(("detected URL list","type");
//add detected URLs
next.add(new CrawlDatums("detected URL list"));

//add one detected URL and return the added URL(CrawlDatum)
//and set its key and type
next.addAndReturn("detected URL").key("key").type("type");
//add detected URLs and return the added URLs(CrawlDatums)
//and set their type and meta info
next.addAndReturn("detected URL list").type("type").meta("page_num",10);

//add detected URL and return next
//and modify the type and meta info of all the CrawlDatums in next,
//including the added URL
next.add("detected URL").type("type").meta("page_num", 10);
//add detected URLs and return next
//and modify the type and meta info of all the CrawlDatums in next,
//including the added URLs
next.add("detected URL list").type("type").meta("page_num", 10);
```

You don't need to consider how to filter duplicated URLs, the crawler will filter them automatically.

## Plugins

Plugins provide a large part of the functionality of WebCollector. There are several kinds of plugins:

+ Executor: Plugins which define how to download webpages, how to parse webpages and how to detect new CrawlDatums(urls)
+ DBManager: Plugins which maintains the crawling history
+ GeneratorFilter: Plugins which generate CrawlDatums(urls) which will be crawled
+ NextFilter: Plugins which filter CrawlDatums(urls) which detected by the crawler

Some BreadthCrawler and RamCrawler are the most used crawlers which extends AutoParseCrawler. The following plugins only work in crawlers which extend AutoParseCrawler:

+ Requester: Plugins which define how to do http request
+ Visitor: Plugins which define how to parse webpages and how to detect new CrawlDatums(urls)

Plugins can be mounted as follows:

```java
crawler.setRequester(xxxxx);
crawler.setVisitor(xxxxx);
crawler.setNextFilter(xxxxx);
crawler.setGeneratorFilter(xxxxx);
crawler.setExecutor(xxxxx);
crawler.setDBManager(xxxxx);
```

AutoParseCrawler is also an Executor plugin, a Requester plugin and a Visitor plugin. By default it use itsself as the Executor plugin, Request Plugin and Visitor plugin. So if you want to write a plugin for AutoParseCrawler, you have two ways:

+ Just override the corresponding methods of your AutoParseCrawler. For example, if you are using BreadthCrawler, all you have to do is override the `Page getResponse(CrawlDatum crawlDatum)` method.
+ Create a new class which implements Requester interface and implement the `Page getResponse(CrawlDatum crawlDatum)` method of the class. Instantiate the class and use `crawler.setRequester(the instance)` to mount the plugin to the crawler.


## Customizing Requester Plugin

Creating a Requester plugin is very easy. You just need to create a new class which implements Requester interface and implement the `Page getResponse(CrawlDatum crawlDatum)` method of the class. [OkHttpRequester](https://github.com/CrawlScript/WebCollector/blob/master/WebCollector/src/main/java/cn/edu/hfut/dmic/webcollector/plugin/net/OkHttpRequester.java) is a Requester Plugin provided by WebCollector. You can find the code here: [OkHttpRequester.class](https://github.com/CrawlScript/WebCollector/blob/master/WebCollector/src/main/java/cn/edu/hfut/dmic/webcollector/plugin/net/OkHttpRequester.java).

Most of the time, you don't need to write a Requester plugin from the scratch. Creating a Requester plugin by extending the OkHttpRequester is a convenient way.

    
## Configuration Details

Configuration mechanism of WebCollector is redesigned in version 2.70. The above example [ManualNewsCrawler.java](https://github.com/CrawlScript/WebCollector/blob/master/ManualNewsCrawler.java) also shows how to use configuration to customize your crawler.

Before version 2.70, configuration is maintained by static variables in class `cn.edu.hfut.dmic.webcollector.util.Config`, hence it's cumbersome to assign different configurations to different crawlers.

Since version 2.70, each crawler can has its own configuration. You can use `crawler.getConf()` to get it or `crawler.setConf(xxx)` to set it. By default, all crawlers use a singleton default configuration, which could be get by Configuration.getDefault(). So in the above example [ManualNewsCrawler.java](https://github.com/CrawlScript/WebCollector/blob/master/ManualNewsCrawler.java), `crawler.getConf().set("xxx", "xxx")` would affect the default configuration, which may be used by other crawlers.

If you want to change the configuration of a crawler without affecting other crawlers, you should manually create a configuration and specify it to the crawler. For example:

```java
Configuration conf = Configuration.copyDefault();

conf.set("test_string_key", "test_string_value");
crawler.getConf().setReadTimeout(1000 * 5);

crawler.setConf(conf);

crawler.getConf().set("test_int_key", 10);
crawler.getConf().setConnectTimeout(1000 * 5);
```

`Configuration.copyDefault()` is suggested, because it creates a copy of the singleton default configuration, which contains some necessary key-value pairs, while `new Configuration()` creates an empty configuration.


## Resumable Crawling

If you want to stop a crawler and continue crawling the next time, you should do two things:

+ Add `crawler.setResumable(true)` to your code.
+ Don't delete the history directory generated by the crawler, which is specified by the crawlPath parameter.

When you call `crawler.start(depth)`, the crawler will delete the history if you set resumable to false, which is false by default. So if you forget to put 'crawler.setResumable(true)' in your code before the first time you start your crawler, it doesn't matter, because you have no history directory. 


## Content Extraction
WebCollector could automatically extract content from news web-pages:

```java
News news = ContentExtractor.getNewsByHtml(html, url);
News news = ContentExtractor.getNewsByHtml(html);
News news = ContentExtractor.getNewsByUrl(url);

String content = ContentExtractor.getContentByHtml(html, url);
String content = ContentExtractor.getContentByHtml(html);
String content = ContentExtractor.getContentByUrl(url);

Element contentElement = ContentExtractor.getContentElementByHtml(html, url);
Element contentElement = ContentExtractor.getContentElementByHtml(html);
Element contentElement = ContentExtractor.getContentElementByUrl(url);
```

<!--

## Other Documentation

+ [中文文档](https://github.com/CrawlScript/WebCollector/blob/master/README.zh-cn.md)
-->