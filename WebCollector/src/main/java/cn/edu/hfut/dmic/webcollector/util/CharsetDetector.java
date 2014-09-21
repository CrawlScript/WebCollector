/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.util;

import org.mozilla.universalchardet.UniversalDetector;

/**
 * 字符集自动检测
 * @author hu
 */
public class CharsetDetector {

    /**
     * 根据字节数组，猜测可能的字符集，如果检测失败，默认返回utf-8
     * @param bytes 待检测的字节数组
     * @return
     */
    public static String guessEncoding(byte[] bytes) {
        String DEFAULT_ENCODING = "UTF-8";
        UniversalDetector detector = new UniversalDetector(null);
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
