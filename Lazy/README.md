##LAZY

LAZY是基于WebCollector的一个简易爬虫，可以通过配置采集网页持久化到mongodb中。

使用方法

+ 进入[LAZY主页](https://github.com/CrawlScript/WebCollector)，下载Lazy-version-bin.zip，解压
+ 下载mongodb，由于国内下载mongodb较慢，提供两个百度网盘下载地址：[Linux 64](http://pan.baidu.com/s/1qXwiEtQ) [Win 64](http://pan.baidu.com/s/1o7sOWcE)
+ 配置并启动mongodb(按照一般流程即可)
+ 进入Lazy-version-bin.zip解压后的文件夹，用命令行执行命令 java -jar Lazy-版本号.jar 配置文件路径，压缩包内自带了一个配置文件。例如我们可以执行：

```
java -jar Lazy-0.1-beta.jar demo_task.json
```

+ 查看mongodb，可以看到爬取的网页