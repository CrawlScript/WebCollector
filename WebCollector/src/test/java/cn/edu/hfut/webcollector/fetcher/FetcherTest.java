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

package cn.edu.hfut.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.fetcher.FSFetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.generator.CollectionGenerator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hu
 */
public class FetcherTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        CollectionFetcher tf=new CollectionFetcher();
        tf.start();
        Thread.sleep(1000);
        tf.stopFetcher();

    }
    
    public static class CollectionFetcher extends Thread{
        
        public void stopFetcher() {
                fetcher.stop();
            }
            
            Fetcher fetcher;

            @Override
            public void run() {
                CollectionGenerator generator = new CollectionGenerator();
                generator.addUrl("http://www.hfut.edu.cn/ch/");
                generator.addUrl("http://news.hfut.edu.cn/");
                for (int i = 0; i < 1000; i++) {
                    String r = "";
                    for (int j = 0; j < i; j++) {
                        r += "#";
                    }
                    generator.addUrl("http://news.hfut.edu.cn/" + r);
                }
                fetcher = new FSFetcher();
                fetcher.setThreads(2);
                try {
                    fetcher.fetchAll(generator);
                } catch (Exception ex) {
                    Logger.getLogger(Fetcher.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
    }
}
