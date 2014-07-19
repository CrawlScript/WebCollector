/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 *
 * @author hu
 */
public class CharsetDetector
{

   public static String guessEncoding(byte[] bytes) {
    String DEFAULT_ENCODING = "UTF-8";
    org.mozilla.universalchardet.UniversalDetector detector =
        new org.mozilla.universalchardet.UniversalDetector(null);
    detector.handleData(bytes, 0, bytes.length);
    detector.dataEnd();
    String encoding = detector.getDetectedCharset();
    detector.reset();
    if (encoding == null) {
        encoding = DEFAULT_ENCODING;
    }
    return encoding;
}
}
