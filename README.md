# pikmin-aggregator

An aggregator for executions of crypto-currency exchanges

[![Build Status](https://travis-ci.org/esplo/pikmin-aggregator.svg?branch=master)](https://travis-ci.org/esplo/pikmin-aggregator)
[![crates.io](https://img.shields.io/crates/v/pikmin-aggregator.svg?style=flat)](https://crates.io/crates/pikmin-aggregator)
[![Documentation](https://docs.rs/pikmin-aggregator/badge.svg)](https://docs.rs/pikmin-aggregator)
[![codecov](https://codecov.io/gh/esplo/pikmin-aggregator/branch/master/graph/badge.svg)](https://codecov.io/gh/esplo/pikmin-aggregator)


This tool collaborates with `pikmin`, which is a downloader for execution data, so that
reduce data sizes by aggregating rows at the same timestamp.

Currently, this only supports MySQL.

## Preparation

This tool uses `OUTFILE/LOAD` for efficient insertion.
Make sure your MySQL instance permits them.

If you use docker, [this issue could be helpful](https://github.com/docker-library/mysql/issues/447). 
