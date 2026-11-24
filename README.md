# NiFi playground

A hands-on Apache NiFi 2.12.0 environment for the workshop: NiFi (single node or a three-node cluster), NiFi Registry,
PostgreSQL, Kafka, Prometheus + Grafana, Splunk, and mock external services for the labs.
Everything starts with a single `make` command, and data survives restarts.

This environment is not meant for production: passwords are for demo purposes only, and certificates are issued by a local CA.

## Requirements

- Docker Desktop (or Docker Engine + Compose v2) with **at least 8 GB** of memory;
- `make`;
- `python3` on the host — only needed for `make lab-final-load`.

## Quick start

```bash
make up            # single NiFi + Registry, Postgres, Kafka, monitoring, lab mock services
make lab-setup     # table and topics for the labs (run once; safe to repeat)
make urls          # URLs and credentials
```

NiFi takes 1–3 minutes to start; `make up` waits until it is ready and grants the required access policies.
Open `https://localhost:8443/nifi`, log in as `admin`, password — from `make urls`.

Instead of a single NiFi you can start a cluster: `make up-cluster` (the cluster needs about 4–5 GB of memory,
so don't run it alongside the single NiFi on an 8 GB machine).

## What's inside

| Service | On the host | From NiFi (`nifi-net` network) | Start |
|---|---|---|---|
| NiFi, single node | `https://localhost:8443/nifi`, `8081` (ListenHTTP, Lab 2) | `https://nifi:8443` | `make nifi` |
| NiFi, 3-node cluster + ZooKeeper | `https://localhost:9443/nifi` (`:9444`, `:9445`) | `https://nifi1:8443` … | `make cluster` |
| NiFi Registry (HTTPS, users in LDAP) | `https://localhost:18443/nifi-registry`; LDAP UI `http://localhost:17170` | `https://nifi-registry:18443` | `make registry` |
| PostgreSQL 18 | `localhost:5432`, DB `nifi`, `nifi` / `nifi` | `postgres:5432` | `make postgres` |
| pgAdmin (PostgreSQL UI) | `http://localhost:5050` (login — `make urls`); the playground server is preconfigured, DB password `nifi` | — | with `make postgres` |
| Kafka (KRaft, 1 broker) + Conduktor | `localhost:9092`; UI `http://localhost:8080` | `kafka1:19092` | `make kafka` |
| Prometheus + kafka-exporter | `http://localhost:9090` | `prometheus:9090` | `make prometheus` |
| Grafana | `http://localhost:3000` (`admin` / `admin`) | — | `make grafana` |
| Splunk Enterprise (trial) | `http://localhost:8000`, search `index=nifi` | TCP `splunk:5514`, HEC `http://splunk:8088` | `make splunk` (not part of `make up`) |
| Lab mock services | — | `customers-api:8000`, `splunk-sim:5514/8088`, `flaky:9099` | `make lab-services` |

Prometheus scrapes metrics from Kafka and from NiFi itself (over mTLS with the `CN=prometheus` client certificate),
and Grafana is already connected to Prometheus. A Registry Client pointing to NiFi Registry is created in NiFi automatically.

## Main commands

| Command | What it does |
|---|---|
| `make help` | All targets with descriptions |
| `make up` / `make up-cluster` | Start the whole environment (with single NiFi / with the cluster) |
| `make <stack>` / `make <stack>-down` | Start / stop a single stack: `nifi`, `cluster`, `registry`, `postgres`, `kafka`, `monitoring`, `prometheus`, `grafana`, `splunk`, `lab-services` |
| `make ps` | Container status |
| `make logs s=<container>` | Logs, e.g. `make logs s=nifi1` |
| `make urls` | URLs and credentials |
| `make down` | Stop everything; data is kept |
| `make destroy` | Remove everything including data (asks for confirmation) |

## Labs

Reference solutions for all workshop labs are in [nifi/flows](nifi/flows/README.md).

```bash
make import f=nifi/flows/lab5-db-rest-kafka.json   # import a flow into the single NiFi
make import-cluster f=nifi/flows/<file>.json        # same for the cluster
make lab03-measure                                  # Lab 3 measurements: split vs record
make lab08-measure                                  # Lab 8 measurements on the imported group: retries, DLQ, API outage (~9 min, .txt + .html)
make lab09-measure                                  # Lab 9 measurements on the imported group: scenarios A-D, 3 runs each (~6 min, .txt + .html)
make lab4-insert n=10                               # new rows for the incremental read in Lab 4
make lab05-data                                     # test orders for Lab 5
make lab-final-load n=100000                        # load for the final lab
make lab-splunk-count                               # how many events the Splunk emulator received
```

After importing, enable the group's Controller Services (*Enable all controller services*) and start it.

## Repository layout

```text
Makefile                  all playground commands
nifi/
  single/  cluster/       NiFi: compose files and authorizers.xml
  registry/               NiFi Registry + LDAP
  common/                 certificates, empty flow, policy and import scripts
  flows/                  reference lab solutions
  certs/                  CA and certificates (generated automatically, not committed to git)
  data/                   = /opt/nifi/data on all NiFi nodes (lab inputs and outputs)
  drivers/                = /opt/nifi/drivers (JDBC drivers; PostgreSQL is already included)
  extensions/             = nar_extensions on all NiFi nodes (custom processor NARs, auto-loaded)
kafka/                    Kafka + Conduktor
postgres/                 PostgreSQL + pgAdmin; init/ — lab tables, pgadmin/ — playground server
monitoring/prometheus/    Prometheus, kafka-exporter, prometheus.yml
monitoring/grafana/       Grafana
splunk/                   Splunk Enterprise and its configuration (index, inputs)
lab-services/             mock services: customers_api.py, splunk_sim.py, flaky.py, produce_logs.py
```

Each compose file is named `docker-compose-<name>.yml` and lives in its stack's folder.

## Good to know

- **One Docker Compose project.** All stacks run as the `nifi-playground` project and communicate over the
  `nifi-net` network. If you run a compose file directly, use the same project name:
  `docker compose -p nifi-playground -f nifi/single/docker-compose-nifi-single.yml up -d`, otherwise Compose will
  create new empty volumes.
- **Data** is stored in `nifi-playground_*` volumes and survives `make down`; only `make destroy` removes it.
- **TLS.** NiFi and Registry run over HTTPS with certificates from the playground CA. To avoid browser warnings,
  import `nifi/certs/ca.crt` as a trusted root certificate.
- **Cluster.** Use the UI through a single node: the login token is issued per node, so switching ports means
  logging in again. Port `8081` (ListenHTTP) is exposed only for the single NiFi.
- **Drivers.** To add a JDBC driver, just drop it into `nifi/drivers` and set the path
  `/opt/nifi/drivers/<file>.jar` in `DBCPConnectionPool` — no restart required.
- **Custom NARs.** Drop a `*.nar` into `nifi/extensions`. Every NiFi node (single and cluster) auto-loads it
  within ~30 seconds, no restart required. NAR files are not committed to git.
- **Re-running Kafka labs.** The consumer group remembers consumed messages; reset it before re-running,
  for example
  `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-lab05`.
- **Splunk** uses an amd64 image (emulated on Apple Silicon); the first start takes a few minutes,
  and the trial license is valid for 60 days.
