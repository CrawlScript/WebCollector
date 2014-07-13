/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.task;

import org.webcollector.model.Page;

/**
 *
 * @author hu
 */
public abstract class Task implements Runnable{
    
    Page page;
    public Task(Page page){
        this.page=page;
    }

    public abstract void execute();
    
    @Override
    public void run() {
        
    }
    
}
