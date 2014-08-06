/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.generator;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Task;

/**
 *
 * @author hu
 */
public abstract class Generator extends Task{
   
           
    public abstract CrawlDatum next();
 
    
}
