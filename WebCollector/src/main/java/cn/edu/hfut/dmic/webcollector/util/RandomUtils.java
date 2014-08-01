/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author hu
 */
public class RandomUtils {
public static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
    public static synchronized String getTimeString(){
      
        return sdf.format(new Date(System.currentTimeMillis()));
    }    
    
    public static void main(String[] args) throws InterruptedException{
        long start=System.currentTimeMillis();
        Thread.sleep(1000);
        long end=System.currentTimeMillis();
        System.out.println(end-start);
    }
}
