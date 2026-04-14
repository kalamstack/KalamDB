Pod::Spec.new do |s|
  s.name             = 'kalam_link'
  s.version          = '0.1.3'
  s.summary          = 'KalamDB client SDK – macOS native (flutter_rust_bridge).'
  s.description      = 'Pre-built dynamic library for the kalam_link Flutter plugin.'
  s.homepage         = 'https://github.com/kalamstack/KalamDB'
  s.license          = { :type => 'MIT', :file => '../LICENSE' }
  s.author           = { 'KalamDB' => 'info@kalamdb.com' }
  s.source           = { :path => '.' }
  s.platform         = :osx, '10.14'

  s.vendored_libraries = 'Libs/libkalam_link_dart.dylib'

  # Dummy Swift file so CocoaPods creates the module map.
  s.source_files = 'Classes/**/*'
  s.swift_version = '5.0'
end
