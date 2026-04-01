package adapter

import "context"

type contextKeyURLTest struct{}

func ContextWithURLTest(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyURLTest{}, true)
}

func IsURLTestFromContext(ctx context.Context) bool {
	return ctx.Value(contextKeyURLTest{}) != nil
}
