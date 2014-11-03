WebCollector
============

###爬虫简介
WebCollector是一个无须配置、便于二次开发的JAVA爬虫框架（内核），它提供精简的的API，只需少量代码即可实现一个功能强大的爬虫。

###爬虫内核：
WebCollector致力于维护一个稳定、可扩的爬虫内核，便于开发者进行灵活的二次开发。内核具有很强的扩展性，用户可以在内核基础上开发自己想要的爬虫。源码中集成了Jsoup，可进行精准的网页解析。 

###MAVEN:
    <dependency>
        <groupId>cn.edu.hfut.dmic.webcollector</groupId>
        <artifactId>WebCollector</artifactId>
        <version>1.36</version>
    </dependency>

###DEMO1：
用WebCollector制作一个爬取《知乎》并进行问题精准抽取的爬虫（JAVA）  

    public class ZhihuCrawler extends BreadthCrawler{
 
        /*visit函数定制访问每个页面时所需进行的操作*/
        @Override
        public void visit(Page page) {
            String question_regex="^http://www.zhihu.com/question/[0-9]+";
            if(Pattern.matches(question_regex, page.getUrl())){
                System.out.println("正在抽取"+page.getUrl());
                /*抽取标题*/
                String title=page.getDoc().title();
                System.out.println(title);
                /*抽取提问内容*/
                String question=page.getDoc().select("div[id=zh-question-detail]").text();
                System.out.println(question);
             
            }
        }
 
        /*启动爬虫*/
        public static void main(String[] args) throws IOException{  
            ZhihuCrawler crawler=new ZhihuCrawler();
            crawler.addSeed("http://www.zhihu.com/question/21003086");
            crawler.addRegex("http://www.zhihu.com/.*");
            crawler.start(5);  
        }
 
   
    }



###DEMO2：
利用WebCollector进行二次开发，定义自己的爬虫



    import cn.edu.hfut.dmic.webcollector.crawler.BreadthCrawler;
    import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
    import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
    import cn.edu.hfut.dmic.webcollector.model.Page;
    import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
    import cn.edu.hfut.dmic.webcollector.net.Request;
    import cn.edu.hfut.dmic.webcollector.net.Response;
    import cn.edu.hfut.dmic.webcollector.parser.ParseData;
    import cn.edu.hfut.dmic.webcollector.parser.ParseResult;
    import cn.edu.hfut.dmic.webcollector.parser.Parser;

    import java.net.URL;
    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;

    import org.apache.http.Header;
    import org.apache.http.HttpEntity;
    import org.apache.http.client.HttpClient;
    import org.apache.http.client.methods.HttpGet;
    import org.apache.http.impl.client.DefaultHttpClient;
    import org.apache.http.util.EntityUtils;
    import org.jsoup.nodes.Document;

    /**
     * 利用WebCollector进行二次开发，定义自己的爬虫
     *
     * @author hu
     */
    public class Demo {

        /**
         * 自定义Http请求
         *
         */
        public static class MyRequest implements Request {

            URL _URL;

            @Override
            public URL getURL() {
                return _URL;
            }

            @Override
            public void setURL(URL url) {
                this._URL = url;
            }

            /**
             * 这里采用httpclient来取代原来的方法，获取http相应，需要导入httpclient4.x的相关jar包
             */
            @Override
            public Response getResponse(CrawlDatum datum) throws Exception {
                /*HttpResponse是一个实现Response的类*/
                HttpResponse response = new HttpResponse(_URL);

                /*通过httpclient来获取http请求的响应信息*/
                HttpClient client = new DefaultHttpClient();
                HttpGet httpGet = new HttpGet(getURL().toString());
                /*这里用的是httpclient的HttpResponse，与WebCollector中的HttpResponse无关*/
                org.apache.http.HttpResponse httpClientResponse = client.execute(httpGet);
                HttpEntity entity = httpClientResponse.getEntity();

                /*
                 将httpclient获取的http响应头信息放入Response
                 Response接口中要求http头是Map<String,List<String>>类型，所以需要做个转换
                 */
                Map<String, List<String>> headers = new HashMap<String, List<String>>();
                for (Header header : httpClientResponse.getAllHeaders()) {
                    List<String> values = new ArrayList<String>();
                    values.add(header.getValue());
                    headers.put(header.getName(), values);
                }
                response.setHeaders(headers);

                /*设置http响应码，必须设置http响应码，否则会影响抓取器对抓取状态的判断*/
                response.setCode(httpClientResponse.getStatusLine().getStatusCode());

                /*设置http响应内容，为网页(文件)的byte数组*/
                response.setContent(EntityUtils.toByteArray(entity));

                /*
                 这里返回的是HttpResponse类型，它的getContentType()方法会自动从getHeader()方法中
                 获取网页响应的content-type,如果自定义Response，一定要实现getContentType()方法，因
                 为网页解析器的生成需要依赖content-type
                 */
                return response;
            }

        }

        /**
         * 自定义一个广度遍历器
         */
        public static class MyCrawler extends BreadthCrawler {

            /**
             * 覆盖Fetcher类的createRequest方法，可以自定义http请求
             * 一般需要自定义一个实现Request接口的类（这里是MyRequest)
             */
            @Override
            public Request createRequest(String url) throws Exception {
                MyRequest request = new MyRequest();
                request.setURL(new URL(url));
                return request;
            }

            /**
             * 这里可以根据http响应的url和contentType来生成网页解析器 contentType可以用来区分相应是网页、图片还是文件
             * 这里直接用父类的方法，可以参照父类的方法，来自己生成需要的网页解析器
             */
            @Override
            public Parser createParser(String url, String contentType) throws Exception {
                return super.createParser(url, contentType);
            }

            /**
             * 定义爬取成功时对页面的操作
             *
             * @param page
             */
            @Override
            public void visit(Page page) {

                System.out.println("---------------------------");

                /*Document是Jsoup的DOM树对象，做网页信息抽取需要依赖Document对象*/
                Document doc = page.getDoc();
                String title = doc.title();
                System.out.println("网页URL:" + page.getUrl());
                System.out.println("网页标题:" + title);

                /*parseResult是在爬取过程中解析的一些简单网页信息*/
                ParseResult parseResult = page.getParseResult();

                /*parseData包括网页的标题、链接以及一些其他信息*/
                ParseData parseData = parseResult.getParsedata();

                if (parseData.getLinks() != null) {
                    System.out.println("网页链接数:" + parseData.getLinks().size());
                }

            }

        }

        public static void main(String[] args) throws Exception {
            /*crawlPath是爬取信息存储的文件夹*/
            String crawlPath = "/home/hu/data/crawl_hfut1";
            MyCrawler crawler = new MyCrawler();
            crawler.setCrawlPath(crawlPath);

            crawler.addSeed("http://news.hfut.edu.cn/");
            crawler.addRegex("http://news.hfut.edu.cn/.*");

            /*禁止爬取带井号的url*/
            crawler.addRegex("-.*#.*");

            /*禁止爬取图片*/
            crawler.addRegex("-.*png.*");
            crawler.addRegex("-.*jpg.*");
            crawler.addRegex("-.*gif.*");
            crawler.addRegex("-.*js.*");
            crawler.addRegex("-.*css.*");

            /*设置线程数*/
            crawler.setThreads(30);

            /*设置为可断点爬取模式*/
            crawler.setResumable(true);

            /*进行深度为3的广度遍历*/
            crawler.start(3);
        }

    }



###WebCollector构架图

![](https://github.com/CrawlScript/WebCollector/raw/master/webcollector_design.png)

    
