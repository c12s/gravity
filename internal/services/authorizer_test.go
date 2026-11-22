package services

import (
	"context"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func ctxWith(t *testing.T, perms string) context.Context {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"permissions": perms}).SignedString([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	return context.WithValue(context.Background(), "authz-token", token)
}

// Gravity cannot see a node's org, so it lets an org-level grant through and
// Magnetar (SetDesiredOsImage) decides whether the org matches the node.
func TestAuthorizeNodeOrAnyOrg(t *testing.T) {
	authz := NewAuthZService("k")
	cases := []struct {
		perms string
		ok    bool
	}{
		{"os.update|node|n1", true},
		{"os.update|org|c12s", true},
		{"os.update|node|n2", false},
		{"node.delete|org|c12s", false},
		{"", false},
	}
	for _, c := range cases {
		err := authz.AuthorizeNodeOrAnyOrg(ctxWith(t, c.perms), "os.update", "n1")
		if (err == nil) != c.ok {
			t.Errorf("%q: got %v, want ok=%v", c.perms, err, c.ok)
		}
	}
}
