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
package cn.edu.hfut.dmic.webcollector.net;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class Proxies extends ArrayList<Proxy> {
    public static final Logger LOG=LoggerFactory.getLogger(Proxies.class);

    private static Random rand = new Random();
    
    public Proxy randomProxy(){
        int r = rand.nextInt(this.size());
        return this.get(r);
    }

//    public Proxies randomProxies(int size){
//        Proxies proxies = new Proxies();
//        ArrayList<Integer> randomIndice = new ArrayList<Integer>();
//        for(int i=0;i<size;i++){
//            randomIndice.add(i);
//        }
//        Collections.shuffle(randomIndice);
//        for(int i=0;i<randomIndice.size();i++){
//            proxies.add(this.get(randomIndice.get(i)));
//        }
//        return proxies;
//    }
    
//    public void addEmpty(){
//        Proxy nullProxy=null;
//        this.add(nullProxy);
//    }

    public void addHttpProxy(String ip, int port) {
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(ip, port));
        this.add(proxy);
    }

    public void addSocksProxy(String ip, int port) {
        Proxy proxy = new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(ip, port));
        this.add(proxy);
    }

//    public void add(String proxyStr) throws Exception {
//        try {
//            String[] infos = proxyStr.split(":");
//            String ip = infos[0];
//            int port = Integer.valueOf(infos[1]);
//
//            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(ip, port));
//            this.add(proxy);
//        } catch (Exception ex) {
//            LOG.info("Exception", ex);
//        }
//    }


//    public void addAllFromFile(File file) throws Exception {
//        FileInputStream fis = new FileInputStream(file);
//        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//        String line = null;
//        while ((line = br.readLine()) != null) {
//            line = line.trim();
//            if (line.startsWith("#")||line.isEmpty()) {
//                continue;
//            } else {
//                this.add(line);
//            }
//        }
//    }
}
