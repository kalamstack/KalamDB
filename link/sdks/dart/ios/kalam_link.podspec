Pod::Spec.new do |s|
  s.name             = 'kalam_link'
  s.version          = '0.1.3'
  s.summary          = 'KalamDB client SDK – iOS native (flutter_rust_bridge).'
  s.description      = 'Pre-built static library for the kalam_link Flutter plugin.'
  s.homepage         = 'https://github.com/kalamstack/KalamDB'
  s.license          = { :type => 'MIT', :file => '../LICENSE' }
  s.author           = { 'KalamDB' => 'info@kalamdb.com' }
  s.source           = { :path => '.' }
  s.platform         = :ios, '12.0'

  s.static_framework = true
  s.vendored_libraries = 'Frameworks/libkalam_link_dart.a'

  # Dummy Swift file so CocoaPods creates the module map. This is required for
  # Flutter's ffiPlugin: true machinery.
  s.source_files = 'Classes/**/*'
  s.swift_version = '5.0'
end
