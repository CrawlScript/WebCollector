package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;

public class StatusGeneratorFilter implements GeneratorFilter {
    @Override
    public CrawlDatum filter(CrawlDatum datum) {
        if(datum.getStatus() == CrawlDatum.STATUS_DB_SUCCESS){
            return null;
        }else{
            return datum;
        }
    }
}
