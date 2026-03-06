/// KalamDB client SDK for Dart and Flutter.
///
/// Provides query execution, live subscriptions, and authentication
/// for KalamDB servers, powered by flutter_rust_bridge.
///
/// ## Quick Start (Flutter)
///
/// ```dart
/// import 'package:kalam_link/kalam_link.dart';
///
/// void main() async {
///   WidgetsFlutterBinding.ensureInitialized();
///   await KalamClient.init();   // load native lib — fast, OK before runApp
///   runApp(MyApp());            // render immediately
/// }
///
/// // Then in your widget/provider:
/// final client = await KalamClient.connect(
///   url: 'https://db.example.com',
///   authProvider: () async => Auth.basic('alice', 'secret123'),
/// );
/// ```
///
/// **Important:** Only `init()` should be awaited before `runApp()`.
/// `connect()` performs network I/O — awaiting it before `runApp()`
/// will freeze the UI until the WebSocket handshake completes.
library;

export 'src/auth.dart';
export 'src/cell_value.dart';
export 'src/file_ref.dart';
export 'src/kalam_client.dart';
export 'src/logger.dart';
export 'src/models.dart';
export 'src/seq_id.dart';
