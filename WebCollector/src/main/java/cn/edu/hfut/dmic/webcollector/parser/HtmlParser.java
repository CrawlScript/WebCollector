/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.parser;


import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import cn.edu.hfut.dmic.webcollector.util.Config;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 *
 * @author hu
 */
public class HtmlParser extends Parser {

    Integer topN;

    public HtmlParser() {
        topN = null;
    }

    public HtmlParser(Integer topN) {
        this.topN = topN;
    }


    @Override
    public ParseResult getParse(Page page) throws UnsupportedEncodingException {
        String url=page.getUrl();
        
        String charset = CharsetDetector.guessEncoding(page.getContent());
        
        String html=new String(page.getContent(), charset);
        page.setHtml(html);
        
        Document doc=Jsoup.parse(page.getHtml());
        doc.setBaseUri(url);      
        page.setDoc(doc);
        
        String title=doc.title();
        String text=doc.text();
     
        ArrayList<Link> links = topNFilter(LinkUtils.getAll(page));
        ParseData parsedata = new ParseData(url,title, links);
        ParseText parsetext=new ParseText(url,text);
        
        return new ParseResult(parsedata,parsetext);
    }

    public ArrayList<Link> topNFilter(ArrayList<Link> origin_links) {
        ArrayList<Link> result=new ArrayList<Link>();
        int updatesize;
        if (topN == null) {
            updatesize = origin_links.size();
        } else {
            updatesize = Math.min(topN, origin_links.size());
        }

        int sum = 0;
        for (int i = 0; i < origin_links.size(); i++) {
            if (sum >= updatesize) {
                break;
            }
            Link link = origin_links.get(i);

            result.add(link);
            sum++;
        }
        return result;
    }

}
