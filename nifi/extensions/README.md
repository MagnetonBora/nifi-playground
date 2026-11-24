# Custom NARs

Put custom processor NARs (`*.nar`) here. The folder is mounted into every NiFi container
(single and cluster) as `/opt/nifi/nifi-current/nar_extensions`, the auto-load directory of the
`apache/nifi` image: a new NAR is picked up within ~30 seconds, no restart needed.

To replace a NAR with a new build of the same version, copy it over the old file and restart NiFi
if the UI still shows the old behaviour.

`*.nar` files are git-ignored.
