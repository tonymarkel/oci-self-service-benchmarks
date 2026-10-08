package frontend

import "testing"

func TestCandidateConsulDialTargets(t *testing.T) {
	for _, domain := range []string{"", "candidate.svc.cluster.local"} {
		s := &Server{ConsulAddr: "consul:8500", KnativeDns: domain}
		for _, name := range []string{"srv-review", "srv-attractions"} {
			want := "consul://consul:8500/" + name
			if domain != "" {
				want += "." + domain
			}
			if got := s.consulDialTarget(name); got != want {
				t.Fatalf("bare/non-Consul target: %s, want %s", got, want)
			}
		}
	}
}
