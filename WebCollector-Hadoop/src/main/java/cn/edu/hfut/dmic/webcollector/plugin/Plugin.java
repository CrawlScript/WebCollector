/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.plugin;

/**
 *
 * @author hu
 */
public class Plugin {
    public static <T> T createPlugin(String className) throws Exception{
        T plugin=(T) Class.forName(className).newInstance();
        return plugin;
    }
}
