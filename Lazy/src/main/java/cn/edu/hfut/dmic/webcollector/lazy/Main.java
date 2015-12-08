/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.lazy;



/**
 *
 * @author hu
 */
public class Main {

    public static void crawl(String[] args) throws Exception {

        String confFileName = args[0];
        LazyConfig lazyConfig = new LazyConfig(confFileName);
        LazyCrawler crawler = new LazyCrawler(lazyConfig);
        crawler.start(lazyConfig.getDepth());
    }

    public static void usage() {
        System.err.println("Usage:Lazy ConfigFileName");
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"demo_task.json"};
        if (args.length == 0) {
            usage();
        }
        crawl(args);

    }
}
