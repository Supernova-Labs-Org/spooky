#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This script only supports Linux."
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Run as root: sudo $0"
  exit 1
fi

declare -A TUNING=(
  ["net.core.rmem_max"]="33554432"
  ["net.core.wmem_max"]="33554432"
  ["net.core.rmem_default"]="8388608"
  ["net.core.wmem_default"]="8388608"
  ["net.core.netdev_max_backlog"]="250000"
  ["net.ipv4.udp_rmem_min"]="16384"
  ["net.ipv4.udp_wmem_min"]="16384"
)

echo "Applying Spooky network sysctl tuning..."
for key in "${!TUNING[@]}"; do
  value="${TUNING[$key]}"
  sysctl -w "${key}=${value}" >/dev/null
  current="$(sysctl -n "$key")"
  echo "${key}=${current}"
done

echo
echo "Done. Persist these values in /etc/sysctl.d/99-spooky-network.conf if needed."
