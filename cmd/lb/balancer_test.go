package main

import (
	"slices"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type BalancerSuite struct{}

var _ = Suite(&BalancerSuite{})

func (s *BalancerSuite) TestIpToHashNumber(c *C) {
	ips := []string{
		"87.154.128.68",
		"55.234.146.40",
		"93.167.203.49",
		"196.16.10.9",
		"106.246.220.17",
	}

	expectedIpBindings := [][]string{
		{"55.234.146.40", "196.16.10.9", "106.246.220.17"},
		{"93.167.203.49"},
		{"87.154.128.68"},
	}

	serversCount := 3

	for _, ip := range ips {
		hashSum, err := ipToHashNumber(ip)
		if err != nil {
			c.Error("unexpected error", err)
		}

		serverIndex := hashSum % uint64(serversCount)

		isValid := slices.Contains(expectedIpBindings[serverIndex], ip)

		c.Assert(isValid, Equals, true, Commentf("expected %s to be in %v", ip, expectedIpBindings[serverIndex]))
	}

	invalidIp := "invalidIp"

	_, err := ipToHashNumber(invalidIp)

	c.Assert(err, NotNil, Commentf("expected error for invalid IP address"))

	ipv6 := "2001:0db8:85a3:0000:0000:8a2e:0370:7334"

	_, err = ipToHashNumber(ipv6)

	c.Assert(err, NotNil, Commentf("expected error for IPv6 address"))
}
