WebCollector
============

### 爬虫简介
WebCollector是一个无须配置、便于二次开发的JAVA爬虫框架（内核），它提供精简的的API，只需少量代码即可实现一个功能强大的爬虫。

### 爬虫内核：
WebCollector致力于维护一个稳定、可扩的爬虫内核，便于开发者进行灵活的二次开发。内核具有很强的扩展性，用户可以在内核基础上开发自己想要的爬虫。源码中集成了Jsoup，可进行精准的网页解析。

### 教程:
WebCollector的开源中国项目主页中可找到教程列表：[http://www.oschina.net/p/webcollector](http://www.oschina.net/p/webcollector)


### 2.x：
WebCollector 2.x版本特性：
 * 1）自定义遍历策略，可完成更为复杂的遍历业务，例如分页、AJAX
 * 2）可以为每个URL设置附加信息(MetaData)，利用附加信息可以完成很多复杂业务，例如深度获取、锚文本获取、引用页面获取、POST参数传递、增量更新等。
 * 3）使用插件机制，WebCollector内置两套插件。
 * 4）内置一套基于内存的插件（RamCrawler)，不依赖文件系统或数据库，适合一次性爬取，例如实时爬取搜索引擎。
 * 5）内置一套基于Berkeley DB（BreadthCrawler)的插件：适合处理长期和大量级的任务，并具有断点爬取功能，不会因为宕机、关闭导致数据丢失。 
 * 6）集成selenium，可以对javascript生成信息进行抽取
 * 7）可轻松自定义http请求，并内置多代理随机切换功能。 可通过定义http请求实现模拟登录。 
 * 8）使用slf4j作为日志门面，可对接多种日志


### Maven

```xml
<dependency>
    <groupId>cn.edu.hfut.dmic.webcollector</groupId>
    <artifactId>WebCollector</artifactId>
    <version>2.72-beta</version>
</dependency>
```


### Jar包
可在[WebCollector的github主页](https://github.com/CrawlScript/WebCollector)下载所需jar包.

+ __webcollector-version-bin.zip__ 包含核心jar包.








