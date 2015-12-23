#!/bin/bash
#mvn package
mvn assembly:assembly

hadoop jar target/WebCollector-Hadoop-0.1-beta-jar-with-dependencies.jar cn.edu.hfut.dmic.webcollector.crawler.Crawler

