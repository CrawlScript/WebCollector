package cn.edu.hfut.dmic.contentextractor;

import org.jsoup.nodes.Document;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by LordLRO on 17.09.2019.
 */

public class ContentExtractorTest {
    private ContentExtractor contentExtractor = new ContentExtractor(new Document("testUri"));

    @Test
    public void testStrSim() throws Exception {
        assertEquals(contentExtractor.strSim("", ""), 0, 0);
        assertEquals(contentExtractor.strSim("", "abc"), 0, 0);
        assertEquals(contentExtractor.strSim("132535365", "123456789"), 0.55555555, 0.00000001);

        String longStringOfLetterA = new String(new char[2000000])
                .replace("\0", "A");
        assertEquals(contentExtractor
                .strSim(longStringOfLetterA, "A"), 1.0 /longStringOfLetterA.length(), 0.000001);
    }

    /**
     * Test using Robustness Testing skill
     */
    @Test public void testLcs() throws Exception {
        try {
            assertEquals(contentExtractor.lcs(null, "Hello World!"), 0);
        }
        catch (NullPointerException e) {
        }

        assertEquals(contentExtractor.lcs("", "Hello World!"), 0);

        assertEquals(contentExtractor.lcs("H", "Hello World!"), 1);

        String firstLongString = new String(new char[1000])
                .replace("\0", "Hello World!");
        assertEquals(contentExtractor.lcs(firstLongString, "Hello World!"), 12);

        String shorterFirstLongString = new String(new char[1000])
                .replace("\0", "Hello World");
        assertEquals(contentExtractor.lcs(shorterFirstLongString, "Hello World!"), 11);

        String longerFirstLongString = new String(new char[1000])
                .replace("\0", "Hello World!!");
        assertEquals(contentExtractor.lcs(longerFirstLongString, "Hello World!"), 12);

        try {
            assertEquals(contentExtractor.lcs("He Word", null), 0);
        }
        catch (NullPointerException e) {
        }

        assertEquals(contentExtractor.lcs("He Word!", ""), 0);

        assertEquals(contentExtractor.lcs("He Word!", "!"), 1);

        String secondLongString = new String(new char[1000])
                .replace("\0", "He Word!");
        assertEquals(contentExtractor.lcs(secondLongString, "He Word!"), 8);

        String shorterSecondLongString = new String(new char[1000])
                .replace("\0", "He Word");
        assertEquals(contentExtractor.lcs(shorterSecondLongString, "He Word!"), 7);

        String longerSecondLongString = new String(new char[1000])
                .replace("\0", "!He Word!");
        assertEquals(contentExtractor.lcs(longerSecondLongString, "He Word!"), 8);

        assertEquals(contentExtractor.lcs("Hello World!", "He Word!"), 8);
    }

}
