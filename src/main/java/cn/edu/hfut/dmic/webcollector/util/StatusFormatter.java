package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;

public interface StatusFormatter {
    String format(CrawlDatum datum);
}
