# ctlstore

ctlstore is a distributed data store that provides very low latency,
always-available, "infinitely" scalable reads. The underlying mechanism for
this is a SQLite database that runs on every host called the LDB. A daemon
called the Reflector plays logged writes from a central database into the LDB.
As this involves replicating the full data store on every host, it is only
practical for situations where the write rate (<100/s total) and data volumes
(<10GB total) are low.

Recommended reading:

* [Rick Branson's blog post on ctlstore](https://segment.com/blog/separating-our-data-and-control-planes-with-ctlstore/)
* [Calvin talking about ctlstore at Synapse](https://vimeo.com/293246627)

## Security

Note that because ctlstore replicates the central database to an LDB on each host 
with a reflector, that LDB contains all of the control data.  In its current state
that means that any application which has access to the LDB can access all of the
data within it.  

The implications of this are that you should not store data in ctlstore that should
only be accessed by a subset of the applications that can read the LDB.  Things like
secrets, passwords, and so on, are an example of this.  

The ctlstore system is meant to store non-sensitive configuration data.

## Development

A MySQL database is needed to run the tests, which can be started using Docker Compose:

```
$ docker-compose up -d
```

Run the tests using make:

```
$ make test
# For more verbosity (`Q=` trick applies to all targets)
$ make test Q=
```

A single `ctlstore` binary is used for all functionality. Build it with make:

```
$ make build
```

Sync non-stdlib dependencies and pull them into `./vendor`

```
$ make deps
```

Ctlstore uses Go modules. To build a docker image, the dependencies must be vendored
first:

```
$ make vendor
```

Many of ctlstore's unit tests use mocks. To regenerate the mocks using [counterfeiter](https://github.com/maxbrunsfeld/counterfeiter):

```
$ make generate
```

## Tying the Pieces Together

This project includes a docker-compose file `docker-compose-example.yml`.  This initializes and runs

* mysql (ctlstore SoR)
* executive service (guards the ctlstore SoR)
* reflector (builds the LDB)
* heartbeat (mutates a ctlstore table periodically)
* sidecar (provides HTTP API access to ctlstore reader API)
* supervisor (periodically snapshots LDB)

To start it, run:

```
$ make deps
$ make vendor
$ docker-compose -f docker-compose-example.yml up -d
```
