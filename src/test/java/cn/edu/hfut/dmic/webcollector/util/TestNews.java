package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.contentextractor.News;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestNews {

    @Test
    public void TestUrlNews() {
        News post = new News();
        String url = "https://datahref.com/";
        post.setUrl(url);
        assertEquals(url, post.getUrl());
    }

}
