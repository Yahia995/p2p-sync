#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
	echo "usage: $0 NAME PORT [extra p2psync flags, e.g. -peers host:port,host:port or -seed host:port]" >&2
	exit 2
fi

name=$1
port=$2
shift 2

root=$(cd "$(dirname "$0")/.." && pwd)
ip=${ADVERTISE_IP:-$(hostname -I | awk '{print $1}')}
dir="$root/nodes/$name"

mkdir -p "$dir"
exec "$root/bin/p2psync" -dir "$dir" -listen "0.0.0.0:$port" -advertise "$ip:$port" "$@"
