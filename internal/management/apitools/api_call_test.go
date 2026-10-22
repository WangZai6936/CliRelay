package apitools

import (
	"net/netip"
	"testing"
)

func TestAPICallAddrBlocked(t *testing.T) {
	allowed := []string{
		"127.0.0.1",
		"10.0.0.1",
		"172.16.0.1",
		"192.168.1.1",
		"::1",
		"fc00::1",
		"fd12:3456:789a:1::1",
	}
	for _, s := range allowed {
		addr := netip.MustParseAddr(s)
		if apiCallAddrBlocked(addr) {
			t.Errorf("%s should be allowed (private/loopback intranet)", s)
		}
	}

	blocked := []string{
		"0.0.0.0",
		"0.0.0.1",
		"100.64.0.1",
		"169.254.169.254",
		"198.18.0.1",
		"224.0.0.1",
		"240.0.0.1",
		"::",
		"fe80::1",
		"ff02::1",
	}
	for _, s := range blocked {
		addr := netip.MustParseAddr(s)
		if !apiCallAddrBlocked(addr) {
			t.Errorf("%s should be blocked", s)
		}
	}
}
