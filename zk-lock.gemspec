# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require 'zk-lock/version'

Gem::Specification.new do |s|
  s.name        = 'zk-lock'
  s.version     = ZKLock::VERSION

  s.authors     = ["Scott Francis"]
  s.email       = ["scott@planetscott.ca"]
  s.summary     = %q{Apache ZooKeeper lock library for Ruby}
  s.description = <<-EOS
A no-frills Zookeeper locking library for Ruby.  For a more full-featured ZooKeeper
implementation, use http://github.com/slyphon/zk.

This library uses version #{ZKLock::DRIVER_VERSION} of zookeeper bindings.

  EOS

  s.homepage    = 'https://github.com/csfrancis/zk-lock'

  s.files         = `git ls-files`.split("\n")
  s.require_paths = ["lib", "ext"]
  s.require_paths += %w[ext]
  s.extensions = 'ext/extconf.rb'

  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
end
