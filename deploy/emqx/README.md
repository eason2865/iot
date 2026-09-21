# EMQX Kubernetes 部署

`cluster.yaml` 是生产 EMQX 长连接集群清单：3 个持久化 Core 节点保存集群状态，2 个无状态 Replicant 节点承接设备连接。客户端应访问 `emqx-listeners.emqx.svc.cluster.local:1883`；Operator 会让该 Service 指向 Replicant 节点。

EMQX 5.9 及以后版本的默认社区许可仅支持单节点。此清单在所有节点从 `emqx-license` Secret 读取集群许可证；未创建该 Secret 时不要应用集群清单。

安装固定版本的 Operator、创建许可证 Secret 后再部署集群：

```bash
curl -fsSL https://github.com/emqx/emqx-operator/releases/download/2.3.0/install.yaml | kubectl apply --server-side=true -f -
kubectl -n emqx create secret generic emqx-license --from-literal=key="$EMQX_LICENSE_KEY"
kubectl apply -f deploy/emqx/cluster.yaml
kubectl wait --for=condition=Ready emqx/emqx -n emqx --timeout=10m
```

生产环境将 `emqx-listeners` 配置到内部或公网 L4 LoadBalancer，并在公网接入前为 MQTT TLS 监听器挂载证书 Secret。Dashboard 保持私网访问，通过受控入口或临时 `kubectl port-forward` 暴露，不能直接公网开放。

## 本地模式

本地使用同一个 Operator、命名空间、Service DNS、PVC 和 listener Service，但应用 [`cluster.local.yaml`](cluster.local.yaml) 的单 Core 节点。默认社区许可只允许该模式，因此它只用于验证连接、共享订阅、重连和发布流程，不提供生产高可用。

`cluster.local.yaml` 的认证回调头含 `${IOT_CORE_MQTT_AUTH_TOKEN}`，这是 **envsubst 占位符**（不是 EMQX 运行时占位符）。必须先用 [`render-local.sh`](render-local.sh) 把 token 字面值渲染进 manifest 再 apply，EMQX 不会对认证器 headers 做环境变量展开：

```bash
scripts/helm-deploy-local.sh   # 先在 emqx namespace 建好 iot-runtime-secrets
sh deploy/emqx/render-local.sh # 渲染 token 并 apply
kubectl wait --for=condition=Ready emqx/emqx -n emqx --timeout=10m
```

注意：本地单节点 license 在滚动更新时会因瞬时双 core 崩溃（`SINGLE_NODE_LICENSE`）。更新 EMQX CR 后执行 `kubectl delete sts -n emqx --all` 让 Operator 重建单节点。

本地 Compose 不运行 EMQX 容器，而是把 `emqx-listeners:1883` 和 `emqx-dashboard:18083` port-forward 到宿主机，使 Docker 内的 demo 与宿主机 MQTT 客户端都走同一套 Kubernetes EMQX Service。
