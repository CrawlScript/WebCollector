#WebCollector
WebCollector is an open source web crawler framework based on Java.It provides
  some simple interfaces for crawling the Web,you can setup a
  multi-threaded web crawler in less than 5 minutes.




##HomePage
[https://github.com/CrawlScript/WebCollector](https://github.com/CrawlScript/WebCollector)



##Installation

### Using Maven

To use the latest release of WebCollector, please use the following snippet in your pom.xml

```xml
    <dependency>
        <groupId>cn.edu.hfut.dmic.webcollector</groupId>
        <artifactId>WebCollector</artifactId>
        <version>2.09</version>
    </dependency>
```

### Without Maven
WebCollector jars are available on the [HomePage](https://github.com/CrawlScript/WebCollector).

+ __webcollector-version-bin.zip__ contains core jars.


##Quickstart
Lets crawl some news from hfut news.This demo prints out the titles and contents extracted from news of hfut news.

[NewsCrawler.java](https://github.com/CrawlScript/WebCollector/blob/master/NewsCrawler.java):

    import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
    import cn.edu.hfut.dmic.webcollector.model.Page;
    import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BreadthCrawler;
    import org.jsoup.nodes.Document;

    /**
     * Crawling news from hfut news
     *
     * @author hu
     */
    public class NewsCrawler extends BreadthCrawler {

        /**
         * @param crawlPath crawlPath is the path of the directory which maintains
         * information of this crawler
         * @param autoParse if autoParse is true,BreadthCrawler will auto extract
         * links which match regex rules from pag
         */
        public NewsCrawler(String crawlPath, boolean autoParse) {
            super(crawlPath, autoParse);
            /*start page*/
            this.addSeed("http://news.hfut.edu.cn/list-1-1.html");

            /*fetch url like http://news.hfut.edu.cn/show-xxxxxxhtml*/
            this.addRegex("http://news.hfut.edu.cn/show-.*html");
            /*do not fetch jpg|png|gif*/
            this.addRegex("-.*\\.(jpg|png|gif).*");
            /*do not fetch url contains #*/
            this.addRegex("-.*#.*");
        }

        @Override
        public void visit(Page page, CrawlDatums next) {
            String url = page.getUrl();
            /*if page is news page*/
            if (page.matchUrl("http://news.hfut.edu.cn/show-.*html")) {
                /*we use jsoup to parse page*/
                Document doc = page.getDoc();

                /*extract title and content of news by css selector*/
                String title = page.select("div[id=Article]>h2").first().text();
                String content = page.select("div#artibody", 0).text();

                System.out.println("URL:\n" + url);
                System.out.println("title:\n" + title);
                System.out.println("content:\n" + content);

                /*If you want to add urls to crawl,add them to nextLink*/
                /*WebCollector automatically filters links that have been fetched before*/
                /*If autoParse is true and the link you add to nextLinks does not match the regex rules,the link will also been filtered.*/
                //next.add("http://xxxxxx.com");
            }
        }

        public static void main(String[] args) throws Exception {
            NewsCrawler crawler = new NewsCrawler("crawl", true);
            crawler.setThreads(50);
            crawler.setTopN(100);
            //crawler.setResumable(true);
            /*start crawl with depth of 4*/
            crawler.start(4);
        }

    }

    


##Content Extraction
WebCollector could automatically extract content from news web-pages:

    News news = ContentExtractor.getNewsByHtml(html, url);
    News news = ContentExtractor.getNewsByHtml(html);
    News news = ContentExtractor.getNewsByUrl(url);

    String content = ContentExtractor.getContentByHtml(html, url);
    String content = ContentExtractor.getContentByHtml(html);
    String content = ContentExtractor.getContentByUrl(url);

    Element contentElement = ContentExtractor.getContentElementByHtml(html, url);
    Element contentElement = ContentExtractor.getContentElementByHtml(html);
    Element contentElement = ContentExtractor.getContentElementByUrl(url);







###Other Documentation

+ [中文文档](https://github.com/CrawlScript/WebCollector/blob/master/README.zh-cn.md)
