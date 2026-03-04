#!/bin/bash

# Check for unused dependencies using cargo-machete
cargo install cargo-machete
cargo machete
