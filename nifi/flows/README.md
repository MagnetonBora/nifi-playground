# Reference lab solutions

Flow Definitions for the workshop labs (Day 1–3 notes) for NiFi 2.12.0, tested on this playground.
Each file is a single Process Group with its own Parameter Context holding the playground addresses.

| File | Lab |
|---|---|
| `lab1-first-flow.json` | Lab 1: NiFi UI and your first flow |
| `lab2-multi-source-routing.json` | Lab 2: multiple sources and routing |
| `lab3-split-vs-record.json` | Lab 3: Split flow vs Record flow |
| `lab4-db-basics.json` | Lab 4: connecting to a database (read, incremental read, write) |
| `lab5-db-rest-kafka.json` | Lab 5: DB + REST + Kafka |
| `lab6-ndjson-pipeline.json` | Lab 6: NDJSON transformation pipeline |
| `lab7-splunk-delivery.json` | Lab 7: delivery to Splunk |
| `lab8-retry-errors.json` | Lab 8: retries and error handling |
| `lab9-performance.json` | Lab 9: performance test |
| `lab10-final-pipeline.json` | Final lab (Block 3.5) |

## Import

```bash
make lab-setup                                    # once: table, topics, directories
make import f=nifi/flows/lab5-db-rest-kafka.json  # into the single NiFi (glob: f="nifi/flows/lab*.json")
make import-cluster f=nifi/flows/<file>.json      # into the cluster
```

Or in the UI: drag the **Process Group** icon onto the canvas and click the file upload icon.
After importing, enable the group's Controller Services (*Enable all controller services*) and start it.

## Where to find what

- Tasks, component settings, verification commands, and common mistakes — in the labs themselves (Day 1–3 notes).
- Playground addresses, test data, and resetting before a re-run — in Appendix A of the notes.
- Starting the playground and `make` commands — in the [repository README](../../README.md).

## Updating the reference solutions

The JSON files are the source of truth. Change the flow in NiFi, export it
(right-click the group → *Download flow definition*), and replace the file in this folder.
