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
        String charset = CharsetDetector.guessEncoding(page.content);
        page.html = new String(page.content, charset);
        page.doc = Jsoup.parse(page.html);
        page.doc.setBaseUri(page.url);
        ArrayList<Link> links = topNFilter(LinkUtils.getAll(page));
        ParseResult parseresult = new ParseResult(page.doc.title(), links);
        return parseresult;
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
