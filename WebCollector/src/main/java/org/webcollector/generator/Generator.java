/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.generator;

import org.webcollector.handler.Handler;

/**
 *
 * @author hu
 */
public abstract class Generator {
    Handler handler;
    public Generator(Handler handler){
        this.handler=handler;
    }
           
    public abstract  void generate();
    
}