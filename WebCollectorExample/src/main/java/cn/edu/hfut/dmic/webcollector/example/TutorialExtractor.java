/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.extract.Extractor;
import cn.edu.hfut.dmic.webcollector.crawler.MultiExtractorCrawler;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.FileSystemOutput;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.JsoupUtils;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.jsoup.nodes.Document;

/**
 * 对于需要抽取多种页面的任务，例如多网站采集新闻，使用抽取器可以很好地进行任务分工
 * 本教程给出一个爬取知乎的例子，整个爬虫包含三种抽取业务：
 * 1) 抽取用户页面的用户信息
 * 2) 抽取问题页面的问题信息
 * 3) 将所有网页保存到文件中
 * 我们分别编写三个抽取器来分别解决这三个问题：
 * 1) PeopleExtractor
 * 2) QuestionExtractor
 * 3) HtmlExtractor
 * 最后在main函数中将三种抽取器加载到爬虫MultiExtractorCrawler中
 * 
 * 可以加载抽取器的爬虫类为MultiExtractorCrawler，由于抽取业务由抽取器完成，所以不需要
 * 用户编写继承MultiExtractorCrawler的类，直接实例化MultiExtractorCrawler，将抽取器直接
 * 加载到MultiExtractorCrawler对象中
 * 
 * 本程序将三种抽取业务的数据分别存储在 download/people download/question 
 * 和 download/html 文件夹中
 * 
 * @author hu
 */
public class TutorialExtractor {

    /**
     * 用于抽取用户信息的抽取器
     */
    public static class PeopleExtractor extends Extractor {

        String name;    //姓名
        String location;    //地址
        String description;    //描述

        public static File dir;
        /*每次抽取都会实例化一个Extractor实例，所以类似计数器这样需要共享的变量，
         可以设置为静态变量*/
        public static AtomicInteger id;

        static {
            dir = new File("download/people");
            if (dir.exists()) {
                FileUtils.deleteDir(dir);
            } else {
                dir.mkdirs();
            }
            id = new AtomicInteger(0);
        }

        public PeopleExtractor(Page page) {
            super(page);
        }

        @Override
        public boolean shouldExecute() {
            /*正则表达式[^/]表示不为"/"的字符，+表示出现0次以上
             我们只希望抽取类似 http://www.zhihu.com/people/mj1997的链接
             需要过滤掉类似  http://www.zhihu.com/people/mj1997/topic/19550780/answers 的链接*/
            return Pattern.matches("http://www.zhihu.com/people/[^/]+", getUrl());
        }

        @Override
        public void extract() throws Exception {
            /*根据css选择器抽取满足条件的第一个元素的文本
             等价于name=selectText("span.name",0)   0表示取第1个元素
             如果想取最后一个元素，改成name=selectText("span.name",-1) */
            name = selectText("span[class=name]");
            /*如果获取不到，则返回缺省值"未知"*/
            location = selectText("span.location.item", "未知");
            description = selectText("span.content", 0, null);
            
            /*这里也可以直接调获取到Page对象*/
            /*上面获取description的代码也可以像下面这样写，但是获取不到元素时会出现异常
               需要加额外代码控制*/
            //description=page.getDoc().select("span.content").first().text();
            
            /*可以通过addNextLinks添加需要后续爬取的页面*/
            //addNextLinks("http://www.sina.com");
            /*可以通过addNextLinksByRegex将页面中所有满足正则的链接加入后续任务。
               注意爬虫会去掉重复的链接（包括和爬取历史重复的链接），也就是说同一个url只会爬取一次*/
            //addNextLinksByRegex("http://www.zhihu.com/.*");
        }

        @Override
        public void output() throws Exception {
            System.out.println("name=" + name + " location=" + location + " description=" + description);
            //保存数据到文件
            String fileName = id.incrementAndGet() + ".txt";
            File outputFile = new File(dir, fileName);
            StringBuilder sb = new StringBuilder();
            sb.append("URL:\n").append(getUrl());
            sb.append("\nNAME:\n").append(name);
            sb.append("\nLOCATION:\n").append(location);
            sb.append("\nDESCRIPTION:\n").append(description);
            String result = sb.toString();
            FileUtils.writeFile(outputFile, result.getBytes("utf-8"));

        }
    }

    public static class QuestionExtractor extends Extractor {

        String title;
        String content;

        public static File dir;
        /*每次抽取都会实例化一个Extractor实例，所以类似计数器这样需要共享的变量，
         可以设置为静态变量*/
        public static AtomicInteger id;

        static {
            dir = new File("download/question");
            if (dir.exists()) {
                FileUtils.deleteDir(dir);
            } else {
                dir.mkdirs();
            }
            id = new AtomicInteger(0);
        }

        public QuestionExtractor(Page page) {
            super(page);
        }

        @Override
        public boolean shouldExecute() {
            return Pattern.matches("http://www.zhihu.com/question/[0-9]+", getUrl());
        }

        @Override
        public void extract() throws Exception {
            title = selectText("div[id=zh-question-title]");
            content = selectText("div#zh-question-detail");
        }

        @Override
        public void output() throws Exception {
            //保存数据到文件
            String fileName = id.incrementAndGet() + ".txt";
            File outputFile = new File(dir, fileName);
            StringBuilder sb = new StringBuilder();
            sb.append("URL:\n").append(getUrl());
            sb.append("\nTITLE:\n").append(title);
            sb.append("\nCONTENT:\n").append(content);
            String result = sb.toString();
            System.out.println(result);
            FileUtils.writeFile(outputFile, result.getBytes("utf-8"));
        }

    }

    /**
     * HtmlExtractor并不执行抽取操作，只是希望将网页源码保存到文件中 只需要进行输出操作，所以只需要在output方法中添加自定义内容
     */
    public static class HtmlExtractor extends Extractor {

        public static FileSystemOutput fsOutput;

        static {
            File dir = new File("download/html");
            if (dir.exists()) {
                FileUtils.deleteDir(dir);
            }
            dir.mkdirs();
            fsOutput = new FileSystemOutput("download/html");
        }

        public HtmlExtractor(Page page) {
            super(page);
        }

        /*HtmlExtractor需要对所有页面执行，所以这里都返回true*/
        @Override
        public boolean shouldExecute() {
            return true;
        }

        @Override
        public void extract() throws Exception {
        }

        @Override
        public void output() throws Exception {
            /*网页中有许多相对引用或链接，处理成绝对引用或链接*/
            JsoupUtils.makeAbs(page.getDoc(), getUrl());
            fsOutput.output(page);
        }

    }

    public static void main(String[] args) throws Exception {
        MultiExtractorCrawler crawler = new MultiExtractorCrawler("crawl", true);
        crawler.addSeed("http://www.zhihu.com/collection/19928423");
        crawler.addRegex("http://www.zhihu.com/people/[^/]*");
        crawler.addRegex("http://www.zhihu.com/question/.*");

        //不希望爬取包含#?的链接，同时不希望爬取jpg或者png文件
        crawler.addRegex("-.*#.*");
        crawler.addRegex("-.*\\?.*");
        crawler.addRegex("-.*\\.jpg.*");
        crawler.addRegex("-.*\\.png.*");

        /*加载抽取器时需要给出适用的url正则，在遇到满足url正则的网页时，抽取器就会在网页上执行*/
        crawler.addExtractor("http://www.zhihu.com/people/[^/]*", PeopleExtractor.class);
        crawler.addExtractor("http://www.zhihu.com/question/[0-9]+", QuestionExtractor.class);

        /*同一个url可以执行多个Extractor，HtmlExtractor会在所有页面上执行*/
        crawler.addExtractor(".*", HtmlExtractor.class);

        crawler.setThreads(100);
        crawler.start(10);
    }

}
