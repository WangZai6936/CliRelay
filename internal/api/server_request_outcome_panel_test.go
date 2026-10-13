package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestOutcomePanelHonorsPanelDisabledAndSecurityHeaders(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, "/manage/runtime/request-outcomes", nil)
		c.Params = gin.Params{{Key: "filepath", Value: "/runtime/request-outcomes"}}
		s := &Server{cfg: &config.Config{RemoteManagement: config.RemoteManagement{DisableControlPanel: disabled}}}
		s.serveManagementControlPanel(c)
		if disabled {
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("disabled panel status=%d", recorder.Code)
			}
			continue
		}
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "请求结果统计") {
			t.Fatal("embedded outcome view unavailable")
		}
		if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("X-Frame-Options") != "SAMEORIGIN" {
			t.Fatal("panel security headers missing")
		}
	}
}

func TestManagementOutcomeEntryDoesNotModifyDistributedAssets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manage.html")
	original := []byte("<!doctype html><body><div id=\"root\"></div></body>")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	serveManagementHTMLWithOutcomeLink(c, path)
	if !strings.Contains(recorder.Body.String(), `href="/manage/runtime/request-outcomes"`) {
		t.Fatal("outcome entry missing")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != string(original) {
		t.Fatal("distributed panel file was modified")
	}
}
