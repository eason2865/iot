#!/usr/bin/env python3
"""Render notification fixtures in Grafana without sending group messages."""

import base64
import datetime
import json
import os
from pathlib import Path
import unittest
import urllib.request


class DingTalkTemplateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = (
            Path(__file__).resolve().parents[1]
            / "monitoring/grafana/notifications/dingtalk.tmpl"
        ).read_text()
        user = os.environ.get("GRAFANA_USER", "admin")
        password = os.environ["GRAFANA_ADMIN_PASSWORD"]
        cls.auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        cls.base = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")

    def alert(self, resolved=False, minimal=False, index=0):
        now = datetime.datetime.now(datetime.timezone.utc)
        labels = {"alertname": f"Template fixture {index}"}
        if not minimal:
            labels.update(severity="warning", service="admin", route="/healthz", status="2xx")
        return {
            "labels": labels,
            "annotations": {
                "summary": "Notification template validation",
                "description": "Synthetic fixture; not a business incident.",
                "__dashboardUid__": "iot-admin-api",
                "__panelId__": "6",
            },
            "startsAt": (now - datetime.timedelta(minutes=10)).isoformat(),
            "endsAt": (now + datetime.timedelta(minutes=-1 if resolved else 10)).isoformat(),
            "generatorURL": self.base + "/alerting/grafana/iot-admin-healthz-qps/view",
        }

    def render(self, alerts):
        payload = {"name": "iot.dingtalk", "template": self.template, "alerts": alerts}
        request = urllib.request.Request(
            self.base + "/api/alertmanager/grafana/config/api/v1/templates/test",
            data=json.dumps(payload).encode(),
            headers={"Authorization": "Basic " + self.auth, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            result = json.load(response)
        self.assertFalse(result.get("errors"), result.get("errors"))
        rendered = {item["name"]: item["text"] for item in result["results"]}
        for text in rendered.values():
            self.assertIn("grafana", text)
            self.assertNotIn("<no value>", text)
        return rendered["iot.dingtalk.title"], rendered["iot.dingtalk.message"]

    def test_firing(self):
        title, body = self.render([self.alert()])
        self.assertIn("告警中", title)
        self.assertIn("[告警中]", body)
        self.assertIn("WARNING", body)
        self.assertIn("/healthz", body)
        self.assertIn("+0800", body)
        self.assertIn("定位图表", body)
        self.assertNotIn("恢复时间", body)

    def test_resolved(self):
        title, body = self.render([self.alert(resolved=True)])
        self.assertIn("已恢复", title)
        self.assertIn("[已恢复]", body)
        self.assertIn("恢复时间", body)
        self.assertNotIn("临时静默", body)

    def test_optional_fields(self):
        _, body = self.render([self.alert(minimal=True)])
        self.assertNotIn("**级别**", body)
        self.assertNotIn("**实例**", body)
        self.assertNotIn("**路由**", body)

    def test_mixed_and_bounded(self):
        title, body = self.render([self.alert(resolved=i % 2 == 1, index=i) for i in range(12)])
        self.assertIn("触发 6 / 恢复 6", title)
        self.assertEqual(body.count("#### "), 10)
        self.assertIn("仅展示前 10 条", body)


if __name__ == "__main__":
    unittest.main()
