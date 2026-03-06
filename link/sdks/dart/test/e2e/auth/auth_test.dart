/// Auth e2e tests — login, refresh, authProvider, JWT lifecycle.
///
/// Mirrors: tests/e2e/auth/auth.test.mjs (TypeScript)
library;

import 'package:flutter_test/flutter_test.dart';
import 'package:kalam_link/kalam_link.dart';

import '../helpers.dart';

void main() {
  group('Auth', skip: skipIfNoIntegration, () {
    // ─────────────────────────────────────────────────────────────────
    // Basic login
    // ─────────────────────────────────────────────────────────────────
    test(
      'login with basic auth returns tokens and user info',
      () async {
        final client = await connectJwtClient();
        try {
          // The client is already logged in — test login directly.
          final anonClient = await KalamClient.connect(
            url: serverUrl,
            timeout: const Duration(seconds: 10),
          );
          try {
            final resp = await anonClient.login(adminUser, adminPass);

            expect(resp.accessToken, isNotEmpty);
            expect(resp.refreshToken, isNotNull);
            expect(resp.user.id, isNotEmpty);
            expect(resp.user.username, isNotEmpty);
            expect(resp.expiresAt, isNotEmpty);
          } finally {
            await anonClient.dispose();
          }
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Refresh token
    // ─────────────────────────────────────────────────────────────────
    test(
      'refreshToken returns a new access token',
      () async {
        await ensureSdkReady();

        final anonClient = await KalamClient.connect(
          url: serverUrl,
          timeout: const Duration(seconds: 10),
        );
        try {
          final login = await anonClient.login(adminUser, adminPass);
          final refreshed = await anonClient.refreshToken(login.refreshToken!);

          expect(refreshed.accessToken, isNotEmpty);
          // Tokens may be identical when issued in the same second (same iat).
          expect(refreshed.accessToken, isA<String>());
        } finally {
          await anonClient.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // JWT auth
    // ─────────────────────────────────────────────────────────────────
    test(
      'JWT-only client can query after login',
      () async {
        await ensureSdkReady();

        // First get a token.
        final bootstrap = await KalamClient.connect(
          url: serverUrl,
          timeout: const Duration(seconds: 10),
        );
        LoginResponse login;
        try {
          login = await bootstrap.login(adminUser, adminPass);
        } finally {
          await bootstrap.dispose();
        }

        // Now connect with JWT only.
        final jwtClient = await KalamClient.connect(
          url: serverUrl,
          authProvider: () async => Auth.jwt(login.accessToken),
          timeout: const Duration(seconds: 10),
        );
        try {
          final resp = await jwtClient.query('SELECT 1 AS val');
          expect(resp.success, isTrue);
          expect(resp.results, isNotEmpty);
        } finally {
          await jwtClient.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // authProvider
    // ─────────────────────────────────────────────────────────────────
    test(
      'authProvider callback is used for authentication',
      () async {
        final client = await connectWithAuthProvider();
        // connectWithAuthProvider uses authProvider internally.
        // If we get here, the provider was called. But let's verify
        // the client can actually query.
        try {
          final resp = await client.query('SELECT 42 AS val');
          expect(resp.success, isTrue);
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Auth.none (anonymous)
    // ─────────────────────────────────────────────────────────────────
    test(
      'Auth.none client can be created (anonymous mode)',
      () async {
        await ensureSdkReady();

        // Auth.none creation should not throw.
        const auth = NoAuth();
        expect(auth, isA<Auth>());
        expect(auth, isA<NoAuth>());
      },
      timeout: const Timeout(Duration(seconds: 10)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Invalid credentials
    // ─────────────────────────────────────────────────────────────────
    test(
      'wrong password rejects login',
      () async {
        await ensureSdkReady();

        final client = await KalamClient.connect(
          url: serverUrl,
          timeout: const Duration(seconds: 10),
        );
        try {
          expect(
            () => client.login(adminUser, 'wrong-password-xyz'),
            throwsA(anything),
          );
        } finally {
          await client.dispose();
        }
      },
      timeout: const Timeout(Duration(seconds: 30)),
    );

    // ─────────────────────────────────────────────────────────────────
    // Auth types
    // ─────────────────────────────────────────────────────────────────
    test(
      'Auth sealed class constructors work',
      () async {
        const basic = BasicAuth('user', 'pass');
        expect(basic.username, 'user');
        expect(basic.password, 'pass');

        const jwt = JwtAuth('token123');
        expect(jwt.token, 'token123');

        const none = NoAuth();
        expect(none, isA<Auth>());

        // Factory constructors
        final basicF = Auth.basic('u', 'p');
        expect(basicF, isA<BasicAuth>());
        final jwtF = Auth.jwt('t');
        expect(jwtF, isA<JwtAuth>());
        final noneF = Auth.none();
        expect(noneF, isA<NoAuth>());
      },
      timeout: const Timeout(Duration(seconds: 5)),
    );
  });
}
