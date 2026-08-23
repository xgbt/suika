package data

import (
	"context"
	stderrors "errors"
	"path/filepath"
	"sync"
	"testing"

	"suika/internal/biz"
	"suika/internal/conf"

	"google.golang.org/protobuf/proto"
)

// newTestCredentialData 构建以全新 sqlite 文件为后端的真实 *Data，
// 关闭转封装以跳过 ffmpeg 探测。
func newTestCredentialData(t *testing.T) *Data {
	t.Helper()
	confData := &conf.Data{
		Database: &conf.Data_Database{Source: filepath.Join(t.TempDir(), "test.db")},
	}
	d, cleanup, err := NewData(confData, &conf.Recorder{RemuxEnabled: proto.Bool(false)})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	return d
}

func TestCredentialRepoGetOnEmptyDB(t *testing.T) {
	repo := NewCredentialRepo(newTestCredentialData(t))

	if _, err := repo.GetCredential(context.Background()); !stderrors.Is(err, biz.ErrCredentialNotFound) {
		t.Fatalf("GetCredential(empty) error = %v, want not found", err)
	}
}

func TestCredentialRepoSaveIsSingletonUpsert(t *testing.T) {
	d := newTestCredentialData(t)
	repo := NewCredentialRepo(d)
	ctx := context.Background()

	if err := repo.SaveCredential(ctx, &biz.Credential{Cookie: "SESSDATA=first", RefreshToken: "rt-1"}); err != nil {
		t.Fatalf("SaveCredential(first) error = %v", err)
	}
	if err := repo.SaveCredential(ctx, &biz.Credential{Cookie: "SESSDATA=second", RefreshToken: "rt-2"}); err != nil {
		t.Fatalf("SaveCredential(second) error = %v", err)
	}

	var count int64
	if err := d.db.Model(&credentialPO{}).Count(&count).Error; err != nil {
		t.Fatalf("count credentials error = %v", err)
	}
	if count != 1 {
		t.Fatalf("credential rows = %d, want exactly 1 after two saves", count)
	}

	got, err := repo.GetCredential(ctx)
	if err != nil {
		t.Fatalf("GetCredential() error = %v", err)
	}
	if got.Cookie != "SESSDATA=second" || got.RefreshToken != "rt-2" {
		t.Fatalf("GetCredential() = %+v, want second credential", got)
	}
}

func TestCredentialRepoHotSwapsCookie(t *testing.T) {
	d := newTestCredentialData(t)
	repo := NewCredentialRepo(d)
	ctx := context.Background()

	if d.Cookie() != "" {
		t.Fatalf("Cookie() before save = %q, want empty", d.Cookie())
	}
	if err := repo.SaveCredential(ctx, &biz.Credential{Cookie: "SESSDATA=live"}); err != nil {
		t.Fatalf("SaveCredential() error = %v", err)
	}
	if d.Cookie() != "SESSDATA=live" {
		t.Fatalf("Cookie() after save = %q, want hot-swapped value", d.Cookie())
	}

	if err := repo.DeleteCredential(ctx); err != nil {
		t.Fatalf("DeleteCredential() error = %v", err)
	}
	if d.Cookie() != "" {
		t.Fatalf("Cookie() after delete = %q, want empty", d.Cookie())
	}
	if _, err := repo.GetCredential(ctx); !stderrors.Is(err, biz.ErrCredentialNotFound) {
		t.Fatalf("GetCredential(after delete) error = %v, want not found", err)
	}
}

func TestCredentialRepoDeleteIdempotent(t *testing.T) {
	repo := NewCredentialRepo(newTestCredentialData(t))

	if err := repo.DeleteCredential(context.Background()); err != nil {
		t.Fatalf("DeleteCredential(first) error = %v", err)
	}
	if err := repo.DeleteCredential(context.Background()); err != nil {
		t.Fatalf("DeleteCredential(second) error = %v, want idempotent nil", err)
	}
}

// TestDataCookieConcurrent 在 -race 下验证 Cookie()/SetCookie() 的并发安全。
func TestDataCookieConcurrent(t *testing.T) {
	d := newTestCredentialData(t)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				_ = d.Cookie()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 2000; j++ {
			d.bili.SetCookie("SESSDATA=v")
		}
	}()
	wg.Wait()
}
