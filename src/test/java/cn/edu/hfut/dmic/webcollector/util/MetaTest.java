package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetaTest {

    @Test
    public void testMetaSetterAndGetter(){

        CrawlDatum datum = new CrawlDatum();
        String key;

        boolean booleanData = true;
        key = "booleanData";
        datum.meta(key, booleanData);
        assertEquals (booleanData, datum.metaAsBoolean(key));

        int intData = 100;
        key = "intData";
        datum.meta(key, intData);
        assertEquals(intData, datum.metaAsInt(key));

        double doubleData = 34.2;
        key = "doubleData";
        datum.meta(key, doubleData);
        assertEquals(doubleData, datum.metaAsDouble(key),0);

        long longData = System.currentTimeMillis();
        key = "longData";
        datum.meta(key, longData);
        assertEquals(longData, datum.metaAsLong(key));


        CrawlDatums datums = new CrawlDatums();
        for(int i=0;i<10;i++){
            datums.add(new CrawlDatum());
        }
        datums.meta(key, longData);
        for(CrawlDatum eachDatum:datums){
            assertEquals(longData, eachDatum.metaAsLong(key));
        }

    }
}
