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
package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.crawler.DeepCrawler;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.Proxys;
import cn.edu.hfut.dmic.webcollector.util.JDBCHelper;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
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
public class TutorialCrawler extends DeepCrawler {

    /*2.x版本中，爬虫的遍历由用户自定义(本质还是广度遍历，但是每个页面
     生成的URL，也就是遍历树中每个节点的孩子节点，是由用户自定义的)。
      
     1.x版本中，默认将每个页面中，所有满足正则约束的链接，都当作待爬取URL，通过
     这种方法可以完成在一定范围内(例如整站)的爬取(根据正则约束)。
    
     所以在2.x版本中，我们只要抽取页面中满足正则的URL，作为Links返回，就可以
     完成1.x版本中BreadthCrawler的功能。
      
     */
    RegexRule regexRule = new RegexRule();

    JdbcTemplate jdbcTemplate = null;

    public TutorialCrawler(String crawlPath) {
        super(crawlPath);

        regexRule.addRule("http://.*zhihu.com/.*");
        regexRule.addRule("-.*jpg.*");

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
                    + ") ENGINE=MyISAM AUTO_INCREMENT=10124512 DEFAULT CHARSET=utf8;");
            System.out.println("成功创建数据表 tb_content");
        } catch (Exception ex) {
            jdbcTemplate = null;
            System.out.println("mysql未开启或JDBCHelper.createMysqlTemplate中参数配置不正确!");
        }
    }

    @Override
    public Links visitAndGetNextLinks(Page page) {
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

        /*下面是2.0版本新加入的内容*/
        /*抽取page中的链接返回，这些链接会在下一轮爬取时被爬取。
         不用担心URL去重，爬虫会自动过滤重复URL。*/
        Links nextLinks = new Links();

        /*我们只希望抽取满足正则约束的URL，
         Links.addAllFromDocument为我们提供了相应的功能*/
        nextLinks.addAllFromDocument(doc, regexRule);

        /*Links类继承ArrayList<String>,可以使用add、addAll等方法自己添加URL
         如果当前页面的链接中，没有需要爬取的，可以return null
         例如如果你的爬取任务只是爬取seed列表中的所有链接，这种情况应该return null
         */
        return nextLinks;
    }

    public static void main(String[] args) throws Exception {
        /*构造函数中的string,是爬虫的crawlPath，爬虫的爬取信息都存在crawlPath文件夹中,
          不同的爬虫请使用不同的crawlPath
        */
        TutorialCrawler crawler = new TutorialCrawler("/home/hu/data/wb");
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

        crawler.start(5);
    }

}
