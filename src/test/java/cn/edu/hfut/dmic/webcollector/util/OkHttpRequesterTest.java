package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.plugin.net.OkHttpRequester;
import org.junit.Test;
import static org.junit.Assert.*;

public class OkHttpRequesterTest {
    OkHttpRequester requester = new OkHttpRequester();

    @Test
    public void testHttpCode(){
        String url = "http://www.hfut.edu.cn/ch/";
        try {
            Page page = requester.getResponse(new CrawlDatum(url));
            assertEquals(200, page.code());
        } catch (Exception e) {
            fail(e.toString());
        }

    }
}
