WebCollector
============

###爬虫简介
WebCollector是一个无须配置、便于二次开发的JAVA爬虫框架（内核），它提供精简的的API，只需少量代码即可实现一个功能强大的爬虫。

###爬虫内核：
WebCollector致力于维护一个稳定、可扩的爬虫内核，便于开发者进行灵活的二次开发。内核具有很强的扩展性，用户可以在内核基础上开发自己想要的爬虫。源码中集成了Jsoup，可进行精准的网页解析。

###1.x：
WebCollector 1.x版本现已转移到[http://git.oschina.net/webcollector/WebCollector-1.x](http://git.oschina.net/webcollector/WebCollector-1.x)维护，建议使用2.x版本。

###2.x：
WebCollector 2.x版本特性：
* 1）自定义遍历策略，可完成更为复杂的遍历业务，例如分页、AJAX
* 2）内置Berkeley DB管理URL，可以处理更大量级的网页
* 3）集成selenium，可以对javascript生成信息进行抽取
* 4）直接支持多代理随机切换
* 5）集成spring jdbc和mysql connector，方便数据持久化
* 6）集成json解析器
* 7）使用slf4j作为日志门面
* 8）修改http请求接口，用户自定义http请求更加方便


WebCollector 2.x教程：
* [WebCollector 2.x tutorial 2 (BreadthCrawler中文教程)](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/TutorialCrawler2.java)
* [WebCollector 2.x 抽取器 (Extractor和MultiExtractorCrawler)](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/TutorialExtractor.java)
* [WebCollector爬取JS生成数据](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoJSCrawler.java)
* [WebCollector爬取搜狗搜索（分页）](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoSogouCrawler.java)
* [WebCollector爬取JSON数据](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoJsonCrawler.java)
* [使用SoupLang脚本同时管理多个页面爬取](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoSoupLangCrawler.java)     [SoupLang脚本](https://github.com/CrawlScript/WebCollector/blob/master/WebCollectorExample/src/main/resources/example/DemoRule1.xml)
* [用WebCollector 2.x爬取新浪微博（无需手动获取cookie)](http://blog.csdn.net/ajaxhu/article/details/42346471)

WebCollector 2.x教程(镜像)：
* [WebCollector 2.x tutorial 2 (BreadthCrawler中文教程)](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/TutorialCrawler2.java)
* [WebCollector 2.x 抽取器 (Extractor和MultiExtractorCrawler)](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/TutorialExtractor.java)
* [WebCollector爬取JS生成数据](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoJSCrawler.java)
* [WebCollector爬取搜狗搜索（分页）](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoSogouCrawler.java)
* [WebCollector爬取JSON数据](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoJsonCrawler.java)
* [使用SoupLang脚本同时管理多个页面爬取](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/java/cn/edu/hfut/dmic/webcollector/example/DemoSoupLangCrawler.java)     [SoupLang脚本](http://git.oschina.net/webcollector/WebCollector/blob/master/WebCollectorExample/src/main/resources/example/DemoRule1.xml)
* [用WebCollector 2.x爬取新浪微博（无需手动获取cookie)](http://blog.csdn.net/ajaxhu/article/details/42346471)



