import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import { execSync } from 'child_process';
import { readFileSync, unlinkSync, existsSync } from 'fs';
import { join } from 'path';
import { requirePassword, URL, PASS } from './helpers.mjs';

requirePassword();

const cliPath = join(import.meta.dirname, '..', 'dist', 'cli.js');
const outFile = join(import.meta.dirname, '..', 'test-output-schema.ts');

after(() => {
  if (existsSync(outFile)) unlinkSync(outFile);
});

describe('CLI', () => {
  it('generates a schema file', () => {
    execSync(`node ${cliPath} --url ${URL} --password ${PASS} --out ${outFile} --include-system`);
    assert.ok(existsSync(outFile));
    const content = readFileSync(outFile, 'utf-8');
    assert.ok(content.includes('pgTable'));
    assert.ok(content.includes('system_users'));
  });

  it('excludes system tables by default', () => {
    execSync(`node ${cliPath} --url ${URL} --password ${PASS} --out ${outFile}`);
    const content = readFileSync(outFile, 'utf-8');
    assert.ok(!content.includes('system_users'));
    assert.ok(!content.includes('dba_'));
  });

  it('exits with error when password is missing', () => {
    assert.throws(
      () => execSync(`node ${cliPath} --url ${URL}`, { stdio: 'pipe' }),
    );
  });

  it('reads password from KALAMDB_PASSWORD env var', () => {
    execSync(`node ${cliPath} --url ${URL} --out ${outFile} --include-system`, {
      env: { ...process.env, KALAMDB_PASSWORD: PASS },
    });
    const content = readFileSync(outFile, 'utf-8');
    assert.ok(content.includes('pgTable'));
    assert.ok(content.includes('system_users'));
  });
});
