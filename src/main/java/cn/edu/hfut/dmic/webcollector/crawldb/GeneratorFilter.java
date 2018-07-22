package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;


public interface GeneratorFilter{
    /**
     * return datum if you want to generate datum
     * return null if you want to filter datum
     * @param datum
     * @return
     */
    CrawlDatum filter(CrawlDatum datum);
}
