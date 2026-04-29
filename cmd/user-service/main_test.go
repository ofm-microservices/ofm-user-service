package main

import (
	"testing"

	"go.uber.org/fx"
)

func TestMainBuildsApp(t *testing.T) {
	original := runApp
	t.Cleanup(func() {
		runApp = original
	})

	called := false
	runApp = func(app *fx.App) {
		if app == nil {
			t.Fatal("app is nil")
		}
		called = true
	}

	main()

	if !called {
		t.Fatal("runApp was not called")
	}
}
