package adminapi

import "testing"

func TestRPCClientConfUsesDirectEndpoints(t *testing.T) {
	t.Setenv("IOT_CORE_ENDPOINTS", "iot-core:9001,iot-core-2:9001")

	conf := rpcClientConf()
	if len(conf.Etcd.Hosts) != 0 || conf.Etcd.Key != "" {
		t.Fatalf("rpc client still configures etcd: %+v", conf.Etcd)
	}
	if len(conf.Endpoints) != 2 || conf.Endpoints[0] != "iot-core:9001" || conf.Endpoints[1] != "iot-core-2:9001" {
		t.Fatalf("direct endpoints = %v", conf.Endpoints)
	}
}
