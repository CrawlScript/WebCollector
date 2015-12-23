/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.plugin.common;

import cn.edu.hfut.dmic.webcollector.fetcher.Visitor;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import org.jsoup.nodes.Document;

/**
 *
 * @author hu
 */
public abstract class RegexVisitor implements Visitor {

    protected RegexRule regexRule=new RegexRule();
    protected boolean autoParse = true;

    public RegexVisitor() {
        config(regexRule);
        autoParse=isAutoParse();
    }

    public abstract void config(RegexRule regexRule);

    public abstract boolean isAutoParse();

  

    @Override
    public void afterVisit(Page page, CrawlDatums next) {
        if (autoParse && !regexRule.isEmpty()) {

            String conteType = page.getResponse().getContentType();
            if (conteType != null && conteType.contains("text/html")) {
                Document doc = page.getDoc();
                if (doc != null) {
                    Links links = new Links().addByRegex(doc, regexRule);
                    next.add(links);
                }
            }
        }
    }

    @Override
    public void fail(Page page, CrawlDatums next) {
    }

    @Override
    public void notFound(Page page, CrawlDatums next) {
    }

}
