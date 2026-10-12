package apitools

import (
	"net/netip"
	"testing"
)

func TestAPICallAddrBlocked_AllowsPrivateAndLoopback(t *testing.T) {
	// Intranet is allowed for management panel fetch (Docker / private / loopback).
	allow := []string{
		"127.0.0.1",
		"::1",
		"10.0.0.5",
		"172.16.0.2",
		"192.168.1.10",
		"fd00::1",
		"8.8.8.8",
		"1.1.1.1",
	}
	for _, s := range allow {
		addr := netip.MustParseAddr(s)
		if apiCallAddrBlocked(addr) {
			t.Fatalf("expected %s allowed, got blocked", s)
		}
	}

	block := []string{
		"0.0.0.0",
		"169.254.169.254",
		"100.64.0.1",
		"198.18.0.1",
		"224.0.0.1",
		"::",
		"fe80::1",
		"ff02::1",
	}
	for _, s := range block {
		addr := netip.MustParseAddr(s)
		if !apiCallAddrBlocked(addr) {
			t.Fatalf("expected %s blocked, got allowed", s)
		}
	}
}
