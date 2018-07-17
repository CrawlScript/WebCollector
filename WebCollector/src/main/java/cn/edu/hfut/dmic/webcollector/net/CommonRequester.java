package cn.edu.hfut.dmic.webcollector.net;

import cn.edu.hfut.dmic.webcollector.conf.DefaultConfigured;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;

public abstract class CommonRequester extends DefaultConfigured implements Requester{
    @Override
    public Page getResponse(String url) throws Exception {
        return getResponse(new CrawlDatum(url));
    }
}
