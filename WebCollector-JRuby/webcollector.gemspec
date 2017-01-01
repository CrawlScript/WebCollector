# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name = 'webcollector'
  gem.version = '0.1.0'

  gem.authors = ['hu']
  gem.email = []
  gem.homepage = 'https://github.com/CrawlScript/WebCollector'
  gem.licenses = ['GPL-3.0']



  gem.files=Dir["lib/*.jar"]+Dir["lib/*.rb"]
  gem.require_paths = ["lib"]
  gem.summary     = "WebCollector-JRuby"
  gem.description = "WebCollector-JRuby"

end
