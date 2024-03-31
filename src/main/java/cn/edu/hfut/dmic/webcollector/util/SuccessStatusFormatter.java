package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;

public class SuccessStatusFormatter implements StatusFormatter{
    public String format(CrawlDatum datum) {
        return "success";
    }
}
