# KVS

This project implements a key-value store database.

I'm roughly following the course schedule for the Pincap Rust Practical Network Applications as a guide. Right now, the kvs crate is a solution to Project 2 of that course, using their tests.

The client-server work implements roughly the specification in Project 3.

## Features

So far, I've completed the following work.

 - Log-file structured storage.
 - Log compaction on threshold of dead data.
 - Synchronous signle-thread server to handle commands over TCP
 - Simple client CLI for sending messages to server.
 - Redis Serialization protocol from-scratch for wire messages.
 - Unit test coverage of existing work

## Roadmap

 - Concurrent server implementation
 - Asynchronous IO on db files
 - Key sharding
 - Distributed consensus with Raft
