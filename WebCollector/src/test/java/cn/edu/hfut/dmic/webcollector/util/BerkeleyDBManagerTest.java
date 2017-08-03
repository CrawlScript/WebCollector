package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.crawldb.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.plugin.berkeley.BerkeleyDBManager;
import org.junit.Test;

import java.util.ArrayList;

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
        while((datum = generator.next())!=null){
            System.out.println(datum);
            System.out.println("====");
            System.out.println(generator.next());
        }



        generator.close();
        dbManager.close();

        dbManager.clear();
    }
}
