/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

import java.net.HttpURLConnection;

/**
 *
 * @author hu
 */
public interface ConnectionConfig {
    public void config(HttpURLConnection con);
    
}
