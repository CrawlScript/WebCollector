package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BerkeleyDBManager;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BerkeleyDBManagerTest {
    @Test
    public void testInject() throws Exception {
        ArrayList<String> urlList = new ArrayList<String>();
        for(int i = 0;i<10;i++){
            urlList.add("https://www.google.com/"+i);
        }

        BerkeleyDBManager dbManager = new BerkeleyDBManager("temp_test_crawldb");
        dbManager.open();
        dbManager.inject(urlList);
        Generator generator = dbManager.createGenerator();
        CrawlDatum datum;
        HashSet<String> generatedUrls = new HashSet<String>();
        while((datum = generator.next())!=null){
            String url = datum.url();
            assertTrue(urlList.contains(url));
            generatedUrls.add(url);
        }
        assertEquals(urlList.size(), generatedUrls.size());
        generator.close();
        dbManager.close();
        dbManager.clear();
    }
}
