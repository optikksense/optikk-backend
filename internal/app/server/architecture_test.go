package server

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAppAndModulesDoNotImportProviderImplementationsDirectly(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve caller path")
	}

	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	targets := []string{
		filepath.Join(repoRoot, "internal", "app", "server"),
		filepath.Join(repoRoot, "internal", "app", "registry"),
		filepath.Join(repoRoot, "internal", "modules"),
	}
	forbidden := []string{
		`"github.com/Optikk-Org/optikk-backend/internal/infra/session"`,
		`"github.com/Optikk-Org/optikk-backend/internal/infra/livetail"`,
		`"github.com/Optikk-Org/optikk-backend/internal/infra/ingestion"`,
		`"github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"`,
	}

	for _, target := range targets {
		err := filepath.Walk(target, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}

			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			content := string(data)
			for _, blocked := range forbidden {
				if strings.Contains(content, blocked) {
					t.Errorf("%s imports forbidden provider package %s", path, blocked)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", target, err)
		}
	}
}
