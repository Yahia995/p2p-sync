#!/usr/bin/env bash
set -euo pipefail

ports=7000-7010

usage() {
	echo "usage: $0 open | close | partition OTHER_IP | heal OTHER_IP" >&2
	exit 2
}

partition_rule() {
	echo "rule family=ipv4 source address=$1 port port=$ports protocol=tcp drop"
}

case "${1:-}" in
open)
	sudo firewall-cmd --add-port=$ports/tcp
	sudo firewall-cmd --permanent --add-port=$ports/tcp
	;;
close)
	sudo firewall-cmd --remove-port=$ports/tcp
	sudo firewall-cmd --permanent --remove-port=$ports/tcp
	;;
partition)
	[ $# -eq 2 ] || usage
	sudo firewall-cmd --add-rich-rule="$(partition_rule "$2")"
	;;
heal)
	[ $# -eq 2 ] || usage
	sudo firewall-cmd --remove-rich-rule="$(partition_rule "$2")"
	;;
*)
	usage
	;;
esac
