require 'java'

jarPaths=File.expand_path('../*.jar', __FILE__)

puts jarPaths

#jarPaths=File.join(File.dirname(File.absolute_path(__FILE__)),"../../java/*.jar")
Dir[jarPaths].each do |jar|
	puts "require jar: "+File.absolute_path(jar)
	require File.absolute_path(jar)
end


module Utils
	include_package Java::cn.edu.hfut.dmic.webcollector.util
	class MysqlTool<MysqlHelper
		def initialize(url:,username:,password:,initialSize:1,maxActive:30)
			super(url,username,password,initialSize,maxActive)
		end
	end
end


module Crawler

	include_package Java::cn.edu.hfut.dmic.webcollector.plugin.berkeley
	include_package Java::cn.edu.hfut.dmic.webcollector.plugin.ram
	include_package Java::cn.edu.hfut.dmic.webcollector.model
	include_package Java::cn.edu.hfut.dmic.webcollector.net

end



