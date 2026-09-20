package core

import "testing"

func TestRPCServerConfDoesNotRegisterWithEtcd(t *testing.T) {
	conf := rpcServerConf()
	if conf.HasEtcd() {
		t.Fatalf("rpc server still configures etcd: %+v", conf.Etcd)
	}
}
