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
import cn.edu.hfut.dmic.webcollector.souplang.SoupLang;
import cn.edu.hfut.dmic.webcollector.util.JDBCHelper;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

/**
 * SoupLang是WebCollector 2.x中的一种爬虫脚本，以Jsoup内置的CSS SELECTOR为基础
 * 程序会将SoupLang的脚本(xml)转换成语义树，所以不用担心配置文件会影响网页抽取的速度。
 * SoupLang除了有Jsoup选择元素、元素属性的功能外，还可以做正则匹配、写数据库等操作
 * 使用SoupLang，可以将多种异构页面的抽取业务统一管理
 * 
 * 本例子使用SoupLang，同时对知乎网站的用户信息、提问信息进行抽取，并同时提交到两张数据表
 * (用户信息表、提问信息表)。
 * 
 * 
 * @author hu
 */
public class DemoSoupLangCrawler extends DeepCrawler {

    public SoupLang soupLang;
    RegexRule regexRule = new RegexRule();

    public DemoSoupLangCrawler(String crawlPath) throws ParserConfigurationException, SAXException, IOException {
        super(crawlPath);
        addSeed("http://www.zhihu.com/");
        regexRule.addRule("http://www.zhihu.com/question/[0-9]+");
        regexRule.addRule("http://www.zhihu.com/people/.+");
        regexRule.addRule("-http://www.zhihu.com/people/.+/.*");
        regexRule.addRule("-.*(jpg|png|gif|#|\\?).*");

        /*soupLang可以从文件、InputStream中读取SoupLang写的抽取脚本
         如果从外部文件读取，soupLang=new SoupLang("文件路径")*/
        soupLang = new SoupLang(ClassLoader.getSystemResourceAsStream("example/DemoRule1.xml"));

    }

    @Override
    public Links visitAndGetNextLinks(Page page) {

        /*soupLang.extract的返回值是一个Context类型的对象,
          对象中存储了SoupLang所有包含name属性的元素，可以通过Context.get
          或者Context.getString()等方法获取*/
        soupLang.extract(page.getDoc());

        /*返回链接，递归爬取*/
        Links nextLinks = new Links();
        nextLinks.addAllFromDocument(page.getDoc(), regexRule);
        return nextLinks;
    }

    public static void main(String[] args) throws Exception {
        try {
            /*用JDBCHelper在JDBCTemplate池中建立一个名为temp1的JDBCTemplate*/
            JDBCHelper.createMysqlTemplate("temp1",
                    "jdbc:mysql://localhost/testdb?useUnicode=true&characterEncoding=utf8",
                    "root", "password", 5, 30);

            JDBCHelper.getJdbcTemplate("temp1").execute("CREATE TABLE IF NOT EXISTS tb_zhihu_question ("
                    + "id int(11) NOT NULL AUTO_INCREMENT,"
                    + "title text,content longtext,"
                    + "PRIMARY KEY (id)"
                    + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
            System.out.println("成功创建数据表 tb_zhihu_question");

            JDBCHelper.getJdbcTemplate("temp1").execute("CREATE TABLE IF NOT EXISTS tb_zhihu_user ("
                    + "id int(11) NOT NULL AUTO_INCREMENT,"
                    + "user varchar(30),url text,"
                    + "PRIMARY KEY (id)"
                    + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
            System.out.println("成功创建数据表 tb_zhihu_question");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("mysql未开启或JDBCHelper.createMysqlTemplate中参数配置不正确!");
            return;
        }

        DemoSoupLangCrawler crawler = new DemoSoupLangCrawler("/home/hu/data/souplang");
        crawler.start(5);

    }

}
