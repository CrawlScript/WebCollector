/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package cn.edu.hfut.dmic.webcollector.util;

import java.util.LinkedList;


/**
 * 老版本使用的线程池，现已废弃
 * @author hu
 */
public class WorkQueue
{
    private final int nThreads;
    private final PoolWorker[] threads;
    private final LinkedList queue;
    public WorkQueue(int nThreads)
    {
        this.nThreads = nThreads;
        queue = new LinkedList();
        threads = new PoolWorker[nThreads];
        for (int i=0; i<nThreads; i++) {
            threads[i] = new PoolWorker();
            threads[i].start();
        }
    }
    
    public boolean isAlive(){
        synchronized(queue){
            if(!queue.isEmpty()){
                return true;
            }
        }
        for(PoolWorker thread:threads){
            if(thread.status==1)
                return true;
        }
        
        
        return false;
    }
    
 
    
    public void killALl(){
        for(int i=0;i<threads.length;i++){
            threads[i].stop();
        }
    }
    
    public void execute(Runnable r) {
        synchronized(queue) {
            queue.addLast(r);
            queue.notify();
        }
    }
    private class PoolWorker extends Thread {
        int status=0;
        public void run() {
            Runnable r;
            while (true) {
                synchronized(queue) {
                    while (queue.isEmpty()) {
                        try
                        {
                            queue.wait();
                        }
                        catch (InterruptedException ignored)
                        {
                        }
                    }
                    r = (Runnable) queue.removeFirst();
                    status=1;
                }
                // If we don't catch RuntimeException, 
                // the pool could leak threads
                try {
                    r.run();
                }
                catch (Exception e) {
                  e.printStackTrace();
                }finally{
                    status=0;
                }
            }
        }
    }
}
