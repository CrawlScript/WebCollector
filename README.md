WebCollector
============

#爬虫简介
WebCollector是一个无须配置、便于二次开发的JAVA爬虫框架（内核），它提供精简的的API，只需少量代码即可实现一个功能强大的爬虫。

#爬虫内核：
WebCollector致力于维护一个稳定、可扩的爬虫内核，便于开发者进行灵活的二次开发。内核具有很强的扩展性，用户可以在内核基础上开发自己想要的爬虫。源码中集成了Jsoup，可进行精准的网页解析。 

#DEMO：
用WebCollector制作一个爬取《知乎》并进行问题精准抽取的爬虫（JAVA）  

    public class ZhihuCrawler extends BreadthCrawler{
 
        /*
            visit函数定制访问每个页面时所需进行的操作
        */
        @Override
        public void visit(Page page) {
            String question_regex="^http://www.zhihu.com/question/[0-9]+";
            if(Pattern.matches(question_regex, page.url)){
                System.out.println("正在抽取"+page.url);
                /*抽取标题*/
                String title=page.doc.title();
                System.out.println(title);
                /*抽取提问内容*/
                String question=page.doc.select("div[id=zh-question-detail]").text();
                System.out.println(question);
             
            }
        }
 
        /*启动爬虫*/
        public static void main(String[] args) throws IOException{  
            ZhihuCrawler crawler=new ZhihuCrawler();
            crawler.addSeed("http://www.zhihu.com/question/21003086");
            crawler.start(5);  
        }
 
   
    }
