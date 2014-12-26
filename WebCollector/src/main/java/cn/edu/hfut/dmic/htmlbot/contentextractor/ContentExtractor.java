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
package cn.edu.hfut.dmic.htmlbot.contentextractor;

import cn.edu.hfut.dmic.htmlbot.DomPage;
import cn.edu.hfut.dmic.htmlbot.HtmlBot;
import cn.edu.hfut.dmic.htmlbot.util.GaussSmooth;
import cn.edu.hfut.dmic.htmlbot.util.JsoupHelper;
import cn.edu.hfut.dmic.htmlbot.util.TextUtils;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.NodeVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by a on 2014/11/2.
 */
public class ContentExtractor {

    public static class CountInfo {

        TextNode tNode;
        public int textCount;
        public int puncCount;

        public CountInfo(TextNode tNode) {
            this.tNode = tNode;
            String text = tNode.text();
            this.textCount = TextUtils.countText(text);
            this.puncCount = TextUtils.countPunc(text);
        }
    }

    public static class ComputeInfo {

        double tpr;
        double ppr;
        double cs;
        double ps;
        double etpr;

        public ComputeInfo(double tpr, double ppr, double cs, double ps) {
            this.tpr = tpr;
            this.ppr = ppr;
            this.cs = cs;
            this.ps = ps;
            this.etpr = tpr * ppr * cs * ps;
        }
    }
    private DomPage domPage;
    private Document doc;
    private ArrayList<TextNode> tNodeList = new ArrayList<TextNode>();
    private HashMap<TextNode, String> xpathMap = new HashMap<TextNode, String>();
    private HashMap<String, ArrayList<CountInfo>> countMap = new HashMap<String, ArrayList<CountInfo>>();
    private HashMap<String, ComputeInfo> computeMap = new HashMap<String, ComputeInfo>();
    private ArrayList<Double> etprList = new ArrayList<Double>();
    private ArrayList<Double> gaussEtprList = new ArrayList<Double>();
    private double threshold;

    private void clean() {
        doc.select("script").remove();
        doc.select("style").remove();
        doc.select("iframe").remove();
    }

    private double computeDeviation(ArrayList<Double> list) {
        if (list.size() == 0) {
            return 0;
        }
        double ave = 0;
        for (Double d : list) {
            ave += d;
        }
        ave = ave / list.size();
        double sum = 0;
        for (Double d : list) {
            sum += (d - ave) * (d - ave);
        }
        sum = sum / list.size();
        return Math.sqrt(sum);
    }

    private double computeThreshold() {
        double d = computeDeviation(gaussEtprList);
        return d * 0.8;
    }

    private ComputeInfo getComputeInfo(ArrayList<CountInfo> countInfoList) {
        double textSum = 0;
        double puncSum = 0;
        ArrayList<Double> textCountList = new ArrayList<Double>();
        ArrayList<Double> puncCountList = new ArrayList<Double>();
        for (CountInfo countInfo : countInfoList) {

            textSum += countInfo.textCount;
            puncSum += countInfo.puncCount;
            textCountList.add(countInfo.textCount + 0.0);
            puncCountList.add(countInfo.puncCount + 0.0);
        }
        double tpr = textSum / countInfoList.size();
        double ppr = puncSum / countInfoList.size();
        double cs = computeDeviation(textCountList);
        double ps = computeDeviation(puncCountList);
        return new ComputeInfo(tpr, ppr, cs, ps);
    }

    private void addTextNode(TextNode tNode) {

        String text = tNode.text().trim();
        if (text.isEmpty()) {
            return;
        }
        String xpath = JsoupHelper.getXpath(tNode);
        tNodeList.add(tNode);
        xpathMap.put(tNode, xpath);

        CountInfo countInfo = new CountInfo(tNode);
        ArrayList<CountInfo> countInfoList = countMap.get(xpath);
        if (countInfoList == null) {
            countInfoList = new ArrayList<CountInfo>();
            countMap.put(xpath, countInfoList);
        }
        countInfoList.add(countInfo);
    }

    private void buildHisto() {
        doc.traverse(new NodeVisitor() {
            @Override
            public void head(Node node, int i) {
                if (node instanceof TextNode) {
                    TextNode tNode = (TextNode) node;
                    addTextNode(tNode);
                }
            }

            @Override
            public void tail(Node node, int i) {

            }
        });
        for (Map.Entry<String, ArrayList<CountInfo>> entry : countMap.entrySet()) {
            ComputeInfo computeInfo = getComputeInfo(entry.getValue());
            computeMap.put(entry.getKey(), computeInfo);
        }

        for (TextNode tNode : tNodeList) {
            String xpath = xpathMap.get(tNode);
            double etpr = computeMap.get(xpath).etpr;
            etprList.add(etpr);
        }
        gaussEtprList = GaussSmooth.gaussSmooth(etprList, 1);
        threshold = computeThreshold();
    }

    public String getContent() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tNodeList.size(); i++) {
            TextNode tNode = tNodeList.get(i);
            double gEtpr = gaussEtprList.get(i);
            if (gEtpr > threshold) {
                sb.append(tNode.text().trim() + "\n");
            }
        }
        return sb.toString();
    }

    /**
     * 从指定url对应的网页中，抽取正文
     * @param url 指定网页的url
     * @return 网页的正文
     * @throws Exception 
     */
    public static String getContentByURL(String url) throws Exception {
        DomPage domPage = HtmlBot.getDomPageByURL(url);
        ContentExtractor contentExtractor = new ContentExtractor(domPage);
        return contentExtractor.getContent();
    }

    /**
     * 从html源码对应的网页中，抽取正文
     * @param html html源码
     * @return 网页的正文
     * @throws Exception 
     */
    public static String getContentByHtml(String html) throws Exception {
        DomPage domPage = HtmlBot.getDomPageByHtml(html);
        ContentExtractor contentExtractor = new ContentExtractor(domPage);
        return contentExtractor.getContent();
    }

    public ContentExtractor(DomPage domPage) {
        this.domPage = domPage;
        this.doc = domPage.getDoc();
        clean();
        buildHisto();
    }

    public static void main(String[] args) throws Exception {

        //String content = ContentExtractor.getContentByURL("http://news.xinhuanet.com/world/2014-11/02/c_127166728.htm");
        String content = ContentExtractor.getContentByURL("http://news.hfut.edu.cn/show-1-14138-1.html");
        System.out.println(content);
    }

}
