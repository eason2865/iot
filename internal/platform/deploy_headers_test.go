package platform

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDeploymentCallbackHeadersPreserveToken(t *testing.T) {
	for _, token := range []string{`quote"token`, `literal\n\t\u0041`, "tab\tnewline\n中文"} {
		cmd := exec.Command("python3", "../../scripts/emqx-auth-headers.py")
		cmd.Env = append(os.Environ(), "IOT_AUTH_CALLBACK_TOKEN="+token)
		out, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		var headers map[string]string
		if err = json.Unmarshal(out, &headers); err != nil || headers["x_iot_auth_token"] != token || headers["content-type"] != "application/json" {
			t.Fatalf("token changed: headers=%q err=%v", headers, err)
		}
	}
}

func TestDeploymentScriptUsesEncodedHeaders(t *testing.T) {
	dir := t.TempDir()
	kubectl := `#!/usr/bin/env python3
import sys,json,os
for a in sys.argv[1:]:
 if a.startswith('--from-literal=EMQX_AUTHN_HEADERS_JSON='):
  open(os.environ['CAPTURE_HEADERS'],'w').write(a.split('=',2)[2])
if 'apply' in sys.argv: sys.stdin.read()
print('{}')
`
	for name, body := range map[string]string{"kubectl": kubectl, "docker": "#!/bin/sh\nexit 1\n", "helm": "#!/bin/sh\nexit 0\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, token := range []string{`quoted"token`, `literal\n\u0041`} {
		capture := filepath.Join(dir, "headers.json")
		cmd := exec.Command("sh", "../../scripts/helm-deploy-local.sh")
		cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "CHECK_EXTERNAL_DEPS=0", "GRPC_TLS_ENABLED=0", "IOT_CORE_MQTT_AUTH_TOKEN="+token, "CAPTURE_HEADERS="+capture)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("script failed: %v %s", err, out)
		}
		raw, err := os.ReadFile(capture)
		if err != nil {
			t.Fatal(err)
		}
		var headers map[string]string
		if err = json.Unmarshal(raw, &headers); err != nil || headers["x_iot_auth_token"] != token {
			t.Fatalf("script corrupted token: err=%v", err)
		}
	}
}
