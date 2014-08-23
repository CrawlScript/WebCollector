/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hu
 */
public class URLUtils {
    public static String getDomain(String url){
        URL _URL=null;
        try {
            _URL=new URL(url);
        } catch (MalformedURLException ex) {
            return null;
        }
        return _URL.getHost();
    }

}
