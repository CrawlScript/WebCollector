
import cn.edu.hfut.dmic.webcollector.crawler.BreadthCrawler;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import java.util.regex.Pattern;
import org.jsoup.nodes.Document;

/**
 * Crawl news from yahoo news
 *
 * @author hu
 */
public class YahooCrawler extends BreadthCrawler {

    /**
     * @param crawlPath crawlPath is the path of the directory which maintains
     * information of this crawler
     * @param autoParse if autoParse is true,BreadthCrawler will auto extract
     * links which match regex rules from pag
     */
    public YahooCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        /*start page*/
        this.addSeed("http://news.yahoo.com/");

        /*fetch url like http://news.yahoo.com/xxxxx*/
        this.addRegex("http://news.yahoo.com/.*");
        /*do not fetch url like http://news.yahoo.com/xxxx/xxx)*/
        this.addRegex("-http://news.yahoo.com/.+/.*");
        /*do not fetch jpg|png|gif*/
        this.addRegex("-.*\\.(jpg|png|gif).*");
        /*do not fetch url contains #*/
        this.addRegex("-.*#.*");
    }

    @Override
    public void visit(Page page, Links nextLinks) {
        String url = page.getUrl();
        /*if page is news page*/
        if (Pattern.matches("http://news.yahoo.com/.+html", url)) {
            /*we use jsoup to parse page*/
            Document doc = page.getDoc();

            /*extract title and content of news by css selector*/
            String title = doc.select("h1[class=headline]").first().text();
            String content = doc.select("div[class=body yom-art-content clearfix]").first().text();

            System.out.println("URL:\n" + url);
            System.out.println("title:\n" + title);
            System.out.println("content:\n" + content);

            /*If you want to add urls to crawl,add them to nextLink*/
            /*WebCollector automatically filters links that have been fetched before*/
            /*If autoParse is true and the link you add to nextLinks does not match the regex rules,the link will also been filtered.*/
            // nextLinks.add("http://xxxxxx.com");
        }
    }

    public static void main(String[] args) throws Exception {
        YahooCrawler crawler = new YahooCrawler("crawl", true);
        crawler.setThreads(50);
        crawler.setTopN(100);
        //crawler.setResumable(true);
            /*start crawl with depth of 4*/
        crawler.start(4);
    }

}
