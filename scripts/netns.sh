#!/usr/bin/env bash
set -euo pipefail

bridge=br-p2p
subnet=10.77.0
port=7001

usage() {
	echo "usage: $0 up COUNT | down | run INDEX [extra p2psync flags] | addr INDEX" >&2
	exit 2
}

up() {
	sudo ip link add "$bridge" type bridge
	sudo ip addr add "$subnet.254/24" dev "$bridge"
	sudo ip link set "$bridge" up
	for i in $(seq 1 "$1"); do
		ns="p2p$i"
		sudo ip netns add "$ns"
		sudo ip link add "veth-p2p$i" type veth peer name veth0 netns "$ns"
		sudo ip link set "veth-p2p$i" master "$bridge" up
		sudo ip -n "$ns" addr add "$subnet.$i/24" dev veth0
		sudo ip -n "$ns" link set veth0 up
		sudo ip -n "$ns" link set lo up
	done
}

down() {
	for ns in $(ip netns list | awk '/^p2p[0-9]+/ {print $1}'); do
		sudo ip netns del "$ns"
	done
	sudo ip link del "$bridge" 2>/dev/null || true
}

run() {
	i=$1
	shift
	root=$(cd "$(dirname "$0")/.." && pwd)
	dir="$root/nodes/ns$i"
	mkdir -p "$dir"
	exec sudo ip netns exec "p2p$i" sudo -u "$USER" "$root/bin/p2psync" \
		-dir "$dir" -listen "$subnet.$i:$port" -advertise "$subnet.$i:$port" "$@"
}

case "${1:-}" in
up)
	[ $# -eq 2 ] || usage
	up "$2"
	;;
down)
	down
	;;
run)
	[ $# -ge 2 ] || usage
	shift
	run "$@"
	;;
addr)
	[ $# -eq 2 ] || usage
	echo "$subnet.$2:$port"
	;;
*)
	usage
	;;
esac
