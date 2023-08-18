# KVS

This project implements a key-value store database.

I'm roughly following the course schedule for the Pingcap Rust Practical Network Applications as a guide. The kvs crate is a solution to Project 2 of that course, using their tests.

The client-server work implements roughly the specification in Project 3.

## Features

So far, I've completed the following work.

 - Redis Serialization protocol (RESP) from-scratch for wire messages and database storage.
 - Log-file structured storage using RESP encoded entries.
 - Automatic log compaction @ 1MB of dead data.
 - Asynchronous server and client pass messages over TCP.
 - Asynchronous db access.
 - Simple client CLI for sending messages to server.
 - Unit testing for Redis Serialization crate.

## Roadmap

 - Tests for server, client, and db crates.
 - Key sharding
 - Distributed consensus with Raft
