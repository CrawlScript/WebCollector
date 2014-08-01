/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.filter;

import cn.edu.hfut.dmic.webcollector.util.Config;

/**
 *
 * @author hu
 */
public class IntervalFilter extends Filter{

    @Override
    public boolean shouldFilter(Object object) {
        Long lasttime=(Long) object;
        if(lasttime+Config.interval>System.currentTimeMillis()){
            return true;
        }
        return false;
    }
    
}
