package api

import (
	"bytes"
	_ "embed"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

//go:embed request_outcome_panel.html
var requestOutcomePanel []byte

// The panel distribution is updated independently of the proxy. Keep this
// additive entry point in source instead of rewriting its compiled bundles.
var requestOutcomePanelLink = []byte(`<a id="clirelay-request-outcomes" href="/manage/runtime/request-outcomes" style="position:fixed;right:20px;bottom:20px;z-index:1000;padding:12px 16px;border-radius:8px;background:#1e293b;color:#fff;font:14px/1.4 system-ui;text-decoration:none;box-shadow:0 2px 8px #0002">请求结果统计</a>`)

func serveManagementHTMLWithOutcomeLink(c *gin.Context, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if !bytes.Contains(data, []byte(`id="clirelay-request-outcomes"`)) {
		data = bytes.Replace(data, []byte("</body>"), append(append([]byte{}, requestOutcomePanelLink...), []byte("</body>")...), 1)
	}
	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Data(http.StatusOK, "text/html; charset=utf-8", data)
}
