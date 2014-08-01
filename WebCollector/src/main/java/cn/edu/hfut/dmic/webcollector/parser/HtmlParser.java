/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import org.jsoup.Jsoup;

/**
 *
 * @author hu
 */
public class HtmlParser extends Parser{

    @Override
    public ParseResult getParse(Page page) throws UnsupportedEncodingException {
         String charset = CharsetDetector.guessEncoding(page.content);
         page.html = new String(page.content, charset);
         page.doc = Jsoup.parse(page.html);
         page.doc.setBaseUri(page.url);
         ArrayList<Link> links=LinkUtils.getAll(page);
         ParseResult parseresult=new ParseResult(page.doc.title(),links);
         return parseresult;
    }
    
    
}
