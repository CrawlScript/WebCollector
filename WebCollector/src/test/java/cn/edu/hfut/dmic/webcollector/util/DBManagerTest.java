package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.crawldb.DBManager;
import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BerkeleyDBManager;
import cn.edu.hfut.dmic.webcollector.plugin.rocks.RocksDBManager;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DBManagerTest {

    public void testInject(DBManager dbManager) throws Exception {

        try {
            ArrayList<String> urlList = new ArrayList<String>();
            for (int i = 0; i < 10; i++) {
                urlList.add("https://www.google.com/" + i);
            }

            dbManager.open();
            dbManager.inject(urlList);
            Generator generator = dbManager.createGenerator(null);

            CrawlDatum datum;
            HashSet<String> generatedUrls = new HashSet<String>();
            while ((datum = generator.next()) != null) {
                String url = datum.url();
                assertTrue(urlList.contains(url));
                generatedUrls.add(url);
            }

            assertEquals(urlList.size(), generatedUrls.size());

            generator.close();
            dbManager.close();
        }finally {
            System.out.println("clear");
            dbManager.clear();
        }

    }


    String tempCrawlPath = "temp_test_crawl";

    @Test
    public void testBerkeleyDBInjector() throws Exception {
        BerkeleyDBManager dbManager = new BerkeleyDBManager(tempCrawlPath);
        testInject(dbManager);
    }

    @Test
    public void testRocksDBInjector() throws Exception {
        RocksDBManager dbManager = new RocksDBManager(tempCrawlPath);
        testInject(dbManager);
    }


}
