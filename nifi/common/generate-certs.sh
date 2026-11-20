#!/bin/sh
# Shared private CA for the playground, written to nifi/certs on the host (mounted at /certs).
#   - ca.crt / ca.key                   certificate authority
#   - truststore.p12                    Java truststore with the CA (used by every NiFi)
#   - <node>.p12                        keystore per NiFi / NiFi Registry host (server + client auth, DN "CN=<node>, OU=NIFI")
#   - <client>.crt / <client>.key       PEM client certificates (e.g. Prometheus, DN "CN=<client>, OU=NIFI")
# Idempotent: only missing items are generated, so new nodes/clients can be added later.
# Demo setup: files are world-readable so containers with different uids can read them.
set -e

OUT=/certs
PASS="${CERT_PASSWORD:?CERT_PASSWORD is required}"
NODES="${NIFI_NODES:-nifi nifi1 nifi2 nifi3 nifi-registry}"
CLIENTS="${CERT_CLIENTS:-prometheus}"
DAYS=3650

cd "$OUT"
umask 022

if [ ! -f ca.key ]; then
  echo "Creating CA"
  openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days $DAYS \
    -subj "/OU=NIFI/CN=NiFi Playground CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
  rm -f truststore.p12
fi

if [ ! -f truststore.p12 ]; then
  echo "Creating truststore"
  keytool -importcert -noprompt -alias ca -file ca.crt \
    -keystore truststore.p12 -storetype PKCS12 -storepass "$PASS"
fi

# sign <name> <extendedKeyUsage> <subjectAltName or empty>
sign() {
  name=$1; eku=$2; san=$3
  openssl req -newkey rsa:2048 -nodes -keyout "$name.key" -out "$name.csr" \
    -subj "/OU=NIFI/CN=$name" 2>/dev/null
  {
    echo "basicConstraints=CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=$eku"
    [ -n "$san" ] && echo "subjectAltName=$san"
  } > "$name.ext"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$name.crt" -days $DAYS -extfile "$name.ext" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
}

for node in $NODES; do
  [ -f "$node.p12" ] && continue
  echo "Creating keystore for NiFi node $node"
  sign "$node" "serverAuth,clientAuth" "DNS:$node,DNS:localhost,IP:127.0.0.1"
  openssl pkcs12 -export -name nifi-key -inkey "$node.key" -in "$node.crt" -certfile ca.crt \
    -out "$node.p12" -passout "pass:$PASS"
  rm -f "$node.key" "$node.crt"
done

for client in $CLIENTS; do
  [ -f "$client.crt" ] && continue
  echo "Creating client certificate for $client"
  sign "$client" "clientAuth" ""
done

chmod 644 ./*
echo "Certificates in $OUT are up to date."
