ReQL-on-FDB
===========

This is a port of RethinkDB that uses FoundationDB as the clustering
layer.  To build, install FoundationDB client libraries onto your
system (e.g. your Linux system) and then use the usual RethinkDB
`./configure --allow-fetch` and `make -j8` commands.

Useful commands:

    rethinkdb create
    rethinkdb create --fdb-wipe
    rethinkdb serve

These expect you to have a FoundationDB instance running and a
FoundationDB configuration file.

This repo is built (forked) off of RethinkDB 2.4.x, at some point
after 2.4.2.

This is *sorely* lacking in documentation.  Questions are welcome.

Missing Features/Differences
============================

Your code will encounter errors if you use:

- change feeds (not supported, but could be),
- system tables (some data is inapplicable, faked for compatibility, or missing),
- shard configuration commands (which are inapplicable), or
- rows larger than 9 megabytes

You might suffer performance regressions.  These would be addressable.
The document size limit of 9 megabytes could also be removed with some
further development.

License
=======

This is licensed under the Affero GPL v3 (unlike RethinkDB, which is
Apache 2).

Contributing
============

Please contact me before working on this, if you want your
contributions not to go to waste.  I might reject all PR's, or require
a CLA much like the one at RethinkDB.com which allows me to relicense
under Apache 2.

Want to see further development on this project?  Email me at
sam@samuelhughes.com.



RethinkDB (Original README below)
======================================

What is RethinkDB?
------------------

* **Open-source** database for building realtime web applications
* **NoSQL** database that stores schemaless JSON documents
* **Distributed** database that is easy to scale
* **High availability** database with automatic failover and robust fault tolerance

RethinkDB is the first open-source scalable database built for
realtime applications. It exposes a new database access model, in
which the developer can tell the database to continuously push updated
query results to applications without polling for changes.  RethinkDB
allows developers to build scalable realtime apps in a fraction of the
time with less effort.

To learn more, check out [rethinkdb.com](https://rethinkdb.com).

Not sure what types of projects RethinkDB can help you build? Here are a few examples:

* Build a [realtime liveblog](https://rethinkdb.com/blog/rethinkdb-pubnub/) with RethinkDB and PubNub
* Create a [collaborative photo sharing whiteboard](https://www.youtube.com/watch?v=pdPRp3UxL_s)
* Build an [IRC bot in Go](https://rethinkdb.com/blog/go-irc-bot/) with RethinkDB changefeeds
* Look at [cats on Instagram in realtime](https://rethinkdb.com/blog/cats-of-instagram/)
* Watch [how Platzi uses RethinkDB](https://www.youtube.com/watch?v=Nb_UzRYDB40) to educate


Quickstart
----------

For a thirty-second RethinkDB quickstart, check out
[rethinkdb.com/docs/quickstart](https://www.rethinkdb.com/docs/quickstart).

Or, get started right away with our ten-minute guide in these languages:

* [**JavaScript**](https://rethinkdb.com/docs/guide/javascript/)
* [**Python**](https://rethinkdb.com/docs/guide/python/)
* [**Ruby**](https://rethinkdb.com/docs/guide/ruby/)
* [**Java**](https://rethinkdb.com/docs/guide/java/)

Besides our four official drivers, we also have many [third-party drivers](https://rethinkdb.com/docs/install-drivers/) supported by the RethinkDB community. Here are a few of them:

* **C#/.NET:** [RethinkDb.Driver](https://github.com/bchavez/RethinkDb.Driver), [rethinkdb-net](https://github.com/mfenniak/rethinkdb-net)
* **C++:** [librethinkdbxx](https://github.com/AtnNn/librethinkdbxx)
* **Clojure:** [clj-rethinkdb](https://github.com/apa512/clj-rethinkdb)
* **Elixir:** [rethinkdb-elixir](https://github.com/rethinkdb/rethinkdb-elixir)
* **Go:** [GoRethink](https://github.com/dancannon/gorethink)
* **Haskell:** [haskell-rethinkdb](https://github.com/atnnn/haskell-rethinkdb)
* **PHP:** [php-rql](https://github.com/danielmewes/php-rql)
* **Rust:** [reql](https://github.com/rust-rethinkdb/reql)
* **Scala:** [rethink-scala](https://github.com/kclay/rethink-scala)

Looking to explore what else RethinkDB offers or the specifics of
ReQL? Check out [our RethinkDB docs](https://rethinkdb.com/docs/) and
[ReQL API](https://rethinkdb.com/api/).

Building
--------

First install some dependencies.  For example, on Ubuntu or Debian:

    sudo apt-get install build-essential protobuf-compiler \
        # python \  # for older distros
        python3 python-is-python3 \
        libprotobuf-dev libcurl4-openssl-dev \
        libncurses5-dev libjemalloc-dev wget m4 g++ libssl-dev

Generally, you will need

* GCC or Clang
* Protocol Buffers
* jemalloc
* Ncurses
* Python 2 or Python 3
* libcurl
* libcrypto (OpenSSL)
* libssl-dev

Then, to build:

    ./configure --allow-fetch
    # or run ./configure --allow-fetch CXX=clang++

    make -j4
    # or run make -j4 DEBUG=1

    sudo make install
    # or run ./build/debug_clang/rethinkdb

See WINDOWS.md and mk/README.md for build instructions for Windows and
FreeBSD.

Need help?
----------

A great place to start is
[rethinkdb.com/community](https://rethinkdb.com/community). Here you
can find out how to ask us questions, reach out to us, or [report an
issue](https://github.com/rethinkdb/rethinkdb/issues). You'll be able
to find all the places we frequent online and at which conference or
meetups you might be able to meet us next.

If you need help right now, you can also find us [on
Slack](https://join.slack.com/t/rethinkdb/shared_invite/enQtNzAxOTUzNTk1NzMzLWY5ZTA0OTNmMWJiOWFmOGVhNTUxZjQzODQyZjIzNjgzZjdjZDFjNDg1NDY3MjFhYmNhOTY1MDVkNDgzMWZiZWM),
[Twitter](https://twitter.com/rethinkdb), or IRC at
[#rethinkdb](irc://chat.freenode.net/#rethinkdb) on Freenode.

Donors
------

* [CNCF](https://www.cncf.io/)
* [Digital Ocean](https://www.digitalocean.com/) provides infrastructure and servers needed for serving mission-critical sites like download.rethinkdb.com or update.rethinkdb.com
* [Atlassian](https://www.atlassian.com/) provides OSS license to be able to handle internal tickets like vulnerability issues
* [Netlify](https://www.netlify.com/) OSS license to be able to migrate rethinkdb.com
* [DNSimple](https://dnsimple.com) provides DNS services for the RethinkDB project
* [ZeroTier](https://www.zerotier.com) sponsored the development of per-table configurable write aggregation including the ability to set write delay to infinite to create a memory-only table ([PR #6392](https://github.com/rethinkdb/rethinkdb/pull/6392))

Licensing
---------

RethinkDB is licensed by the Linux Foundation under the open-source
Apache 2.0 license. Portions of the software are licensed by Google
and others and used with permission or subject to their respective
license agreements.

Where's the changelog?
----------------------
We keep [a list of changes and feature explanations here](NOTES.md).
