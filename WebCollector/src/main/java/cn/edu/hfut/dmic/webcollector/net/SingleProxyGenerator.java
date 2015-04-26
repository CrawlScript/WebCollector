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

/**
 *
 * @author hu
 */
public class SingleProxyGenerator implements ProxyGenerator {

    protected Proxy proxy = null;

    public SingleProxyGenerator() {

    }

    public SingleProxyGenerator(Proxy proxy) {
        this.proxy = proxy;
    }

    public SingleProxyGenerator(String host, int port, Proxy.Type type) {
        proxy = new Proxy(type, new InetSocketAddress(host, port));
    }
    
    public SingleProxyGenerator(String host, int port) {
        proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port));
    }

    @Override
    public Proxy next(String url) {
        return proxy;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }

    @Override
    public void markBad(Proxy proxy, String url) {
    }

    @Override
    public void markGood(Proxy proxy, String url) {
    }

}
