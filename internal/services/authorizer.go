package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type AuthZService struct {
	key string
}

func NewAuthZService(key string) AuthZService {
	return AuthZService{key: key}
}

func (s AuthZService) Authorize(ctx context.Context, permName string, objKind string, objId string) error {
	permissions, err := s.permissions(ctx)
	if err != nil {
		return err
	}
	reqPerm := fmt.Sprintf("%s|%s|%s", permName, objKind, objId)
	for _, perm := range permissions {
		if perm == reqPerm {
			return nil
		}
	}
	return errors.New("required permission not found")
}

// AuthorizeNodeOrAnyOrg passes a per-node grant, or any org-level grant of
// the permission. Gravity does not know which org a node belongs to; the
// service that records the intent (Magnetar.SetDesiredOsImage) checks that
// the org grant matches the node's org before anything reaches the node.
func (s AuthZService) AuthorizeNodeOrAnyOrg(ctx context.Context, permName string, nodeId string) error {
	permissions, err := s.permissions(ctx)
	if err != nil {
		return err
	}
	nodePerm := fmt.Sprintf("%s|node|%s", permName, nodeId)
	orgPrefix := permName + "|org|"
	for _, perm := range permissions {
		if perm == nodePerm || strings.HasPrefix(perm, orgPrefix) {
			return nil
		}
	}
	return errors.New("required permission not found")
}

func (s AuthZService) permissions(ctx context.Context) ([]string, error) {
	tokenString, ok := ctx.Value("authz-token").(string)
	if !ok {
		return nil, errors.New("no token provided")
	}
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return []byte(s.key), nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims type")
	}
	permissionsClaim, ok := claims["permissions"].(string)
	if !ok {
		return nil, errors.New("custom claim permissions is not a string or does not exist")
	}
	return strings.Split(permissionsClaim, ","), nil
}
