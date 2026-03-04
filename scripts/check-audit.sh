#!/bin/bash

# Audit Cargo dependencies for security vulnerabilities
cargo install cargo-audit
cargo audit
