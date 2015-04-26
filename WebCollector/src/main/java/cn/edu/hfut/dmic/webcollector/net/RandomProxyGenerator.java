/*
 * Copyright (C) 2015 hu
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
package cn.edu.hfut.dmic.webcollector.net;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Random;

/**
 *
 * @author hu
 */
public class RandomProxyGenerator implements ProxyGenerator {
    
    protected Proxys proxys = null;
    
    public RandomProxyGenerator() {
        proxys = new Proxys();
    }
    
    public RandomProxyGenerator(Proxys proxys) {
        this.proxys = proxys;
    }
    
    protected final Object lock = new Object();
    
    public void addProxy(Proxy proxy) {
        synchronized (lock) {
            proxys.add(proxy);
        }
    }
    
    public void removeProxy(Proxy proxy) {
        synchronized (lock) {
            proxys.remove(proxy);
        }
    }
    
    Random random = new Random();
    
    @Override
    public Proxy next(String url) {
        synchronized (lock) {
            if (proxys == null) {
                return null;
            }
            if (proxys.isEmpty()) {
                return null;
            }
            if (proxys.size() == 1) {
                return proxys.get(0);
            }
            try {
                int r = random.nextInt(proxys.size());
                return proxys.get(r);
            } catch (Exception ex) {
                return null;
            }
        }
        
    }
    
    public Proxys getProxys() {
        return proxys;
    }
    
    public void setProxys(Proxys proxys) {
        this.proxys = proxys;
    }
    
    public void addProxy(String host, int port, Proxy.Type type) {
        addProxy(new Proxy(type, new InetSocketAddress(host, port)));
    }
    
    public void addProxy(String host, int port) {
        addProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port)));
    }
    
    @Override
    public void markBad(Proxy proxy, String url) {
        
    }
    
    @Override
    public void markGood(Proxy proxy, String url) {
    }
    
}
