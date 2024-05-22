package integration

import (
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type BalancerSuite struct{}

var (
	_      = Suite(&BalancerSuite{})
	client = http.Client{
		Timeout: 3 * time.Second,
	}
	baseAddress = "http://127.0.0.1:8090"
	servers     = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

func (s *BalancerSuite) TestIpToHashNumber(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		c.Skip("Integration test is not enabled")
	}

	ips := []string{
		"87.154.128.68",
		"55.234.146.40",
		"93.167.203.49:8080",
		"196.16.10.9",
		"106.246.220.17:2121",
	}

	expectedIpBindings := map[string][]string{
		"server1:8080": {"55.234.146.40", "196.16.10.9", "106.246.220.17:2121"},
		"server2:8080": {"93.167.203.49:8080"},
		"server3:8080": {"87.154.128.68"},
	}

	getCorrectBinding := func(ip string) string {
		for _, server := range servers {
			if slices.Contains(expectedIpBindings[server], ip) {
				return server
			}
		}

		panic(fmt.Sprintf("cannot find binding for %s", ip))
	}

	wg := sync.WaitGroup{}

	for _, ip := range ips {
		wg.Add(1)

		go func(ip string) {
			defer wg.Done()

			req, _ := http.NewRequest("GET", baseAddress, nil)

			req.Header.Set("X-Forwarded-For", ip)

			resp, fetchErr := client.Do(req)

			if fetchErr != nil {
				c.Fatal(fetchErr)
			}

			lbFrom := resp.Header.Get("lb-from")
			binding, found := expectedIpBindings[lbFrom]

			if !found {
				c.Errorf("unexpected lb-from header value: %s", lbFrom)
			}

			isValid := slices.Contains(binding, ip)
			c.Assert(isValid, Equals, true, Commentf("expected %s to be in %v, got %v", ip, getCorrectBinding(ip), lbFrom))
		}(ip)
	}

	wg.Wait()
}

func BenchmarkBalancer(b *testing.B) {
	// TODO: Реалізуйте інтеграційний бенчмарк для балансувальникка.
}
