package platform

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// gRPC mTLS 配置。iot-core（服务端）与 management-api（客户端）读取相同的三个
// 路径变量，分别指向各自的角色证书；三个变量都不设置时保持明文（本地裸跑默认）。
const (
	grpcTLSCertEnv       = "IOT_CORE_TLS_CERT"
	grpcTLSKeyEnv        = "IOT_CORE_TLS_KEY"
	grpcTLSCAEnv         = "IOT_CORE_TLS_CA"
	grpcTLSServerNameEnv = "IOT_CORE_TLS_SERVER_NAME"
)

// GRPCServerTLSCredentials 加载 iot-core gRPC 服务端凭证，要求并校验客户端证书。
// 未配置任何 TLS 环境变量时返回 (nil, nil)，表示保持明文。
func GRPCServerTLSCredentials() (credentials.TransportCredentials, error) {
	certPath, keyPath, caPath := grpcTLSPaths()
	if certPath == "" && keyPath == "" && caPath == "" {
		return nil, nil
	}
	if certPath == "" || keyPath == "" || caPath == "" {
		return nil, fmt.Errorf("gRPC TLS 需要同时设置 %s、%s、%s", grpcTLSCertEnv, grpcTLSKeyEnv, grpcTLSCAEnv)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("加载 gRPC 服务端证书失败: %w", err)
	}
	pool, err := loadCertPool(caPath)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// GRPCClientTLSCredentials 加载 management-api 侧的客户端凭证，校验服务端证书
// 并出示客户端证书。未配置任何 TLS 环境变量时返回 (nil, nil)，表示保持明文。
func GRPCClientTLSCredentials() (credentials.TransportCredentials, error) {
	certPath, keyPath, caPath := grpcTLSPaths()
	if certPath == "" && keyPath == "" && caPath == "" {
		return nil, nil
	}
	if certPath == "" || keyPath == "" || caPath == "" {
		return nil, fmt.Errorf("gRPC TLS 需要同时设置 %s、%s、%s", grpcTLSCertEnv, grpcTLSKeyEnv, grpcTLSCAEnv)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("加载 gRPC 客户端证书失败: %w", err)
	}
	pool, err := loadCertPool(caPath)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   os.Getenv(grpcTLSServerNameEnv),
		MinVersion:   tls.VersionTLS12,
	}), nil
}

func grpcTLSPaths() (certPath, keyPath, caPath string) {
	return os.Getenv(grpcTLSCertEnv), os.Getenv(grpcTLSKeyEnv), os.Getenv(grpcTLSCAEnv)
}

func loadCertPool(caPath string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("读取 gRPC CA 证书失败: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("gRPC CA 证书 %s 不含有效 PEM 证书", caPath)
	}
	return pool, nil
}
