package cn.edu.hfut.dmic.webcollector.conf;

public class DefaultConfigured extends CommonConfigured{
    public DefaultConfigured() {
        setConf(Configuration.getDefault());
    }
}
