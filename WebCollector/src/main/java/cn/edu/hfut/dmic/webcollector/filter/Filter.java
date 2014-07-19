/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.filter;



/**
 *
 * @author hu
 */
public abstract class Filter {
    public abstract boolean shouldFilter(Object object);
    
}
