package platform

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTestCerts 生成一套临时 CA + 服务端/客户端证书，返回文件路径。
func writeTestCerts(t *testing.T) (caPath, serverCert, serverKey, clientCert, clientKey string) {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成 CA 私钥失败: %v", err)
	}
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("生成 CA 证书失败: %v", err)
	}
	caPath = filepath.Join(dir, "ca.crt")
	writePEM(t, caPath, "CERTIFICATE", caDER)

	serverCert, serverKey = writeLeafCert(t, dir, caDER, caKey, "iot-core", []string{"iot-core"}, 2)
	clientCert, clientKey = writeLeafCert(t, dir, caDER, caKey, "management-api", nil, 3)
	return caPath, serverCert, serverKey, clientCert, clientKey
}

func writeLeafCert(t *testing.T, dir string, caDER []byte, caKey *ecdsa.PrivateKey, cn string, dnsNames []string, serial int64) (certPath, keyPath string) {
	t.Helper()
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("解析 CA 证书失败: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("签发证书失败: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("编码私钥失败: %v", err)
	}
	certPath = filepath.Join(dir, cn+".crt")
	keyPath = filepath.Join(dir, cn+".key")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("写入 %s 失败: %v", path, err)
	}
}

func TestGRPCTLSCredentialsDisabledByDefault(t *testing.T) {
	serverCreds, err := GRPCServerTLSCredentials()
	if err != nil || serverCreds != nil {
		t.Fatalf("未设置环境变量时服务端应返回 (nil, nil)，得到 (%v, %v)", serverCreds, err)
	}
	clientCreds, err := GRPCClientTLSCredentials()
	if err != nil || clientCreds != nil {
		t.Fatalf("未设置环境变量时客户端应返回 (nil, nil)，得到 (%v, %v)", clientCreds, err)
	}
}

func TestGRPCTLSCredentialsPartialEnvFails(t *testing.T) {
	t.Setenv(grpcTLSCertEnv, "cert.pem")
	if _, err := GRPCServerTLSCredentials(); err == nil {
		t.Fatal("只设置部分变量时服务端应报错")
	}
	if _, err := GRPCClientTLSCredentials(); err == nil {
		t.Fatal("只设置部分变量时客户端应报错")
	}
}

// TestGRPCTLSCredentialsMutualHandshake 用 net.Pipe 验证双方凭证能完成 mTLS 握手。
func TestGRPCTLSCredentialsMutualHandshake(t *testing.T) {
	caPath, serverCert, serverKey, clientCert, clientKey := writeTestCerts(t)
	t.Setenv(grpcTLSCAEnv, caPath)

	t.Setenv(grpcTLSCertEnv, serverCert)
	t.Setenv(grpcTLSKeyEnv, serverKey)
	serverCreds, err := GRPCServerTLSCredentials()
	if err != nil {
		t.Fatalf("加载服务端凭证失败: %v", err)
	}

	t.Setenv(grpcTLSCertEnv, clientCert)
	t.Setenv(grpcTLSKeyEnv, clientKey)
	t.Setenv(grpcTLSServerNameEnv, "iot-core")
	clientCreds, err := GRPCClientTLSCredentials()
	if err != nil {
		t.Fatalf("加载客户端凭证失败: %v", err)
	}

	srvConn, cliConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, _, err := serverCreds.ServerHandshake(srvConn)
		errCh <- err
	}()
	if _, _, err := clientCreds.ClientHandshake(context.Background(), "iot-core", cliConn); err != nil {
		t.Fatalf("客户端握手失败: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("服务端握手失败: %v", err)
	}
}

// TestGRPCTLSCredentialsRejectsClientWithoutCert 服务端必须拒绝不出示证书的客户端。
func TestGRPCTLSCredentialsRejectsClientWithoutCert(t *testing.T) {
	caPath, serverCert, serverKey, _, _ := writeTestCerts(t)
	t.Setenv(grpcTLSCertEnv, serverCert)
	t.Setenv(grpcTLSKeyEnv, serverKey)
	t.Setenv(grpcTLSCAEnv, caPath)
	serverCreds, err := GRPCServerTLSCredentials()
	if err != nil {
		t.Fatalf("加载服务端凭证失败: %v", err)
	}

	pool := x509.NewCertPool()
	pemBytes, _ := os.ReadFile(caPath)
	pool.AppendCertsFromPEM(pemBytes)

	srvConn, cliConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, _, err := serverCreds.ServerHandshake(srvConn)
		errCh <- err
	}()
	// 固定 TLS 1.2 让客户端握手同步失败（TLS 1.3 下服务端告警在握手后才到达）。
	conn := tls.Client(cliConn, &tls.Config{
		RootCAs:    pool,
		ServerName: "iot-core",
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS12,
	})
	if err := conn.Handshake(); err == nil {
		t.Fatal("无客户端证书的连接应被服务端拒绝")
	}
	if err := <-errCh; err == nil {
		t.Fatal("服务端应返回客户端证书校验错误")
	}
}
