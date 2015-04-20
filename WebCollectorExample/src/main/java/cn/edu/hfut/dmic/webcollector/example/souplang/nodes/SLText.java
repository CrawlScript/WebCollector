package cn.edu.hfut.dmic.webcollector.example.souplang.nodes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import cn.edu.hfut.dmic.webcollector.example.souplang.Context;
import cn.edu.hfut.dmic.webcollector.example.souplang.LangNode;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class SLText extends LangNode {

    public static final Logger LOG = LoggerFactory.getLogger(SLText.class);
    public String regex = null;
    public Integer group = null;

    public void readGroup(org.w3c.dom.Element xmlElement) {
        String groupAttr = xmlElement.getAttribute("group");
        if (!groupAttr.isEmpty()) {
            group = Integer.valueOf(groupAttr);
        }
    }

    public void readRegex(org.w3c.dom.Element xmlElement) {
        regex = xmlElement.getAttribute("regex");
        if (regex.isEmpty()) {
            regex = null;
        }
    }

    public String extractByRegex(String inputStr) {
        if (regex == null) {
            return inputStr;
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(inputStr);
        if (matcher.find()) {
            if (group == null) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    sb.append(matcher.group(i));
                    if (i != matcher.groupCount()) {
                        sb.append(" ");
                    }
                }
                return sb.toString();
            } else {
                return matcher.group(group);
            }

        } else {
            return null;
        }
    }

    @Override
    public Object process(Object input,Context context) throws InputTypeErrorException {
        if (input == null) {
            return null;
        }
        String result = null;
        if (input instanceof Element) {
            Element jsoupElement = (Element) input;
            result = jsoupElement.text();
        } else if (input instanceof Elements) {
            Elements jsoupElements = (Elements) input;
            result = jsoupElements.text();
        } else if (input instanceof TextNode) {
            TextNode jsoupTextNode = (TextNode) input;
            result = jsoupTextNode.text();
        }
        if (result == null) {
            result = input.toString();
        }
        return extractByRegex(result);
    }

    @Override
    public boolean validate(Object input) throws Exception {
        return true;
    }

}
