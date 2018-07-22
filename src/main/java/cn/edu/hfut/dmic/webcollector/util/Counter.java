/*
 * Copyright (C) 2017 hu
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author hu
 */
public class Counter {
    protected AtomicInteger data;

    public Counter() {
        this(0);
    }
    
    public Counter(int initValue) {
        data=new AtomicInteger(initValue);
    }
    
    public int inc(){
        return data.incrementAndGet();
    }
    public int inc(int num){
        return data.addAndGet(num);
    }
    
    public void set(int value){
        data.set(value);
    }
    
    public int get(){
        return data.get();
    }
    
}
