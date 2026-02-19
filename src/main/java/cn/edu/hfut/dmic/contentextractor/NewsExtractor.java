package cn.edu.hfut.dmic.contentextractor;

import cn.edu.hfut.dmic.webcollector.plugin.net.OkHttpRequester;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewsExtractor {
    News news=new News();

    public static OkHttpRequester okHttpRequester = new OkHttpRequester();
    private static final Logger LOG = LoggerFactory.getLogger(NewsExtractor.class);

    private Document doc;
    private ContentExtractor contentExtractor;

    public NewsExtractor(Document doc) {
        this.contentExtractor = new ContentExtractor(doc);
    }

    public News getNews() throws Exception {
        News news = new News();
        Element contentElement;
        try {
            contentElement = contentExtractor.getContentElement();
            news.setContentElement(contentElement);
        } catch (Exception ex) {
            LOG.info("news content extraction failed,extraction abort", ex);
            throw new Exception(ex);
        }

        if (doc.baseUri() != null) {
            news.setUrl(doc.baseUri());
        }

        try {
            news.setTime(contentExtractor.getTime(contentElement));
        } catch (Exception ex) {
            LOG.info("news title extraction failed", ex);
        }

        try {
            news.setTitle(contentExtractor.getTitle(contentElement));
        } catch (Exception ex) {
            LOG.info("title extraction failed", ex);
        }
        return news;
    }

    public static News getNewsByDoc(Document doc) throws Exception {
        NewsExtractor newsExtractor=new NewsExtractor(doc);
        return newsExtractor.getNews();
    }

    /*输入HTML，获取结构化新闻信息*/
    public static News getNewsByHtml(String html) throws Exception {
        Document doc = Jsoup.parse(html);
        return getNewsByDoc(doc);
    }

    /*输入HTML和URL，获取结构化新闻信息*/
    public static News getNewsByHtml(String html, String url) throws Exception {
        Document doc = Jsoup.parse(html, url);
        return getNewsByDoc(doc);
    }

}
