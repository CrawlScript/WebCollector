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

package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.crawler.BreadthCrawler;
import cn.edu.hfut.dmic.webcollector.example.util.JDBCHelper;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.Proxys;
import org.jsoup.nodes.Document;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * WebCollector 2.x版本的tutorial
 * 2.x版本特性：
 *   1）自定义遍历策略，可完成更为复杂的遍历业务，例如分页、AJAX
 *   2）内置Berkeley DB管理URL，可以处理更大量级的网页
 *   3）集成selenium，可以对javascript生成信息进行抽取
 *   4）直接支持多代理随机切换
 *   5）集成spring jdbc和mysql connection，方便数据持久化
 *   6）集成json解析器
 *   7）使用slf4j作为日志门面
 *   8）修改http请求接口，用户自定义http请求更加方便
 * 
 * 可在cn.edu.hfut.dmic.webcollector.example包中找到例子(Demo)
 * 
 * @author hu
 */
public class TutorialCrawler2 extends BreadthCrawler {

    /**
     * 用户自定义对每个页面的操作，一般将抽取、持久化等操作写在visit方法中。
     * @param page
     * @param nextLinks 需要后续爬取的URL。如果autoParse为true，爬虫会自动抽取符合正则的链接并加入nextLinks。
     */
    @Override
    public void visit(Page page, Links nextLinks) {
        Document doc = page.getDoc();
        String title = doc.title();
        System.out.println("URL:" + page.getUrl() + "  标题:" + title);

        /*将数据插入mysql*/
        if (jdbcTemplate != null) {
            int updates=jdbcTemplate.update("insert into tb_content (title,url,html) value(?,?,?)",
                    title, page.getUrl(), page.getHtml());
            if(updates==1){
                System.out.println("mysql插入成功");
            }
        }
        
        /*
        //添加到nextLinks的链接会在下一层或下x层被爬取，爬虫会自动对URL进行去重，所以用户在编写爬虫时完全不必考虑生成重复URL的问题。
        //如果这里添加的链接已经被爬取过，则链接不会在后续任务中被爬取
        //如果需要强制添加已爬取过的链接，只能在爬虫启动（包括断点启动）时，通过Crawler.addForcedSeed强制加入URL。
         nextLinks.add("http://www.csdn.net");
        */
    }
    

    JdbcTemplate jdbcTemplate = null;

    /*如果autoParse设置为true，遍历器会自动解析页面中符合正则的链接，加入后续爬取任务，否则不自动解析链接。*/
    public TutorialCrawler2(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
        
        /*BreadthCrawler可以直接添加URL正则规则*/
        this.addRegex("http://.*zhihu.com/.*");
        this.addRegex("-.*jpg.*");
        
        /*创建一个JdbcTemplate对象,"mysql1"是用户自定义的名称，以后可以通过
         JDBCHelper.getJdbcTemplate("mysql1")来获取这个对象。
         参数分别是：名称、连接URL、用户名、密码、初始化连接数、最大连接数
        
         这里的JdbcTemplate对象自己可以处理连接池，所以爬虫在多线程中，可以共用
         一个JdbcTemplate对象(每个线程中通过JDBCHelper.getJdbcTemplate("名称")
         获取同一个JdbcTemplate对象)             
         */
        try {
            jdbcTemplate = JDBCHelper.createMysqlTemplate("mysql1",
                    "jdbc:mysql://localhost/testdb?useUnicode=true&characterEncoding=utf8",
                    "root", "password", 5, 30);

            /*创建数据表*/
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS tb_content ("
                    + "id int(11) NOT NULL AUTO_INCREMENT,"
                    + "title varchar(50),url varchar(200),html longtext,"
                    + "PRIMARY KEY (id)"
                    + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
            System.out.println("成功创建数据表 tb_content");
        } catch (Exception ex) {
            jdbcTemplate = null;
            System.out.println("mysql未开启或JDBCHelper.createMysqlTemplate中参数配置不正确!");
        }
        
    }

   
   

    

    public static void main(String[] args) throws Exception {
        /*
           第一个参数是爬虫的crawlPath，crawlPath是维护URL信息的文件夹的路径，如果爬虫需要断点爬取，每次请选择相同的crawlPath
           第二个参数表示是否自动抽取符合正则的链接并加入后续任务
        */
        TutorialCrawler2 crawler = new TutorialCrawler2("/home/hu/data/wb",true);
        crawler.setThreads(50);
        crawler.addSeed("http://www.zhihu.com/");
        crawler.setResumable(false);

        /*2.x版本直接支持多代理随机切换*/
        Proxys proxys = new Proxys();
        /*
         可用代理可以到 http://www.brieftools.info/proxy/ 获取
         添加代理的方式:
         1)ip和端口
         proxys.add("123.123.123.123",8080);
         2)文件
         proxys.addAllFromFile(new File("xxx.txt"));
         文件内容类似:
         123.123.123.123:90
         234.234.324.234:8080
         一个代理占一行
         */

        crawler.setProxys(proxys);

        /*设置是否断点爬取*/
        crawler.setResumable(false);
        /*设置每层爬取爬取的最大URL数量*/
        crawler.setTopN(100);

        /*如果希望尽可能地爬取，这里可以设置一个很大的数，爬虫会在没有待爬取URL时自动停止*/
        crawler.start(5);
    }

    

}
