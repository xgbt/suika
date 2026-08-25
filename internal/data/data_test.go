package data

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"suika/internal/conf"
)

func TestSQLiteFilePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      string
		wantPath    string
		wantErr     bool
		errContains string
	}{
		{name: "empty", source: "", wantErr: true, errContains: "database source is empty"},
		{name: "plain relative", source: "./data/suika.db", wantPath: filepath.Clean("./data/suika.db")},
		{name: "file prefix relative", source: "file:./data/suika.db", wantPath: filepath.Clean("./data/suika.db")},
		{name: "file prefix absolute", source: "file:/tmp/suika.db", wantPath: "/tmp/suika.db"},
		{name: "bare file prefix rejected", source: "file:", wantErr: true, errContains: "invalid database source"},
		{name: "file uri with authority rejected", source: "file:///tmp/suika.db", wantErr: true, errContains: "authority"},
		{name: "query is rejected", source: "./data/suika.db?cache=shared", wantErr: true, errContains: "query parameters are not supported"},
		{name: "trailing slash rejected", source: "./data/", wantErr: true, errContains: "directory path"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotPath, err := sqliteFilePath(tt.source)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("sqliteFilePath(%q) expected error, got nil", tt.source)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("sqliteFilePath(%q) error = %q, want contains %q", tt.source, err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("sqliteFilePath(%q) unexpected error: %v", tt.source, err)
			}
			if filepath.Clean(gotPath) != filepath.Clean(tt.wantPath) {
				t.Fatalf("sqliteFilePath(%q) path = %q, want %q", tt.source, gotPath, tt.wantPath)
			}
		})
	}
}

func TestEnsureSQLiteDirCreatesParentDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dbPath := filepath.Join(root, "nested", "db", "rooms.db")

	gotPath, err := ensureSQLiteDir(dbPath)
	if err != nil {
		t.Fatalf("ensureSQLiteDir returned error: %v", err)
	}
	if filepath.Clean(gotPath) != filepath.Clean(dbPath) {
		t.Fatalf("ensureSQLiteDir path = %q, want %q", gotPath, dbPath)
	}

	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Fatalf("expected parent dir to exist, got err: %v", err)
	}
}

func TestEnsureSQLiteDirRejectsQuerySource(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dbPath := filepath.Join(root, "memory", "db", "rooms.db")
	source := "file:" + dbPath + "?cache=shared"

	_, err := ensureSQLiteDir(source)
	if err == nil {
		t.Fatalf("ensureSQLiteDir(%q) expected error, got nil", source)
	}
	if !strings.Contains(err.Error(), "query parameters are not supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewDataInitializesRoomsTableWhenDBFileExists(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dbPath := filepath.Join(root, "state", "rooms.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir parent dir: %v", err)
	}
	if err := os.WriteFile(dbPath, []byte{}, 0o644); err != nil {
		t.Fatalf("create empty db file: %v", err)
	}

	d, cleanup, err := NewData(
		&conf.Data{Database: &conf.Data_Database{Source: dbPath}},
		&conf.Recorder{},
	)
	if err != nil {
		t.Fatalf("NewData returned error: %v", err)
	}
	defer cleanup()

	if !d.db.Migrator().HasTable(&roomPO{}) {
		t.Fatalf("expected rooms table to be initialized")
	}
}
