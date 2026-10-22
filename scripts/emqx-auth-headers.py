#!/usr/bin/env python3
"""Encode the callback secret from the environment without shell JSON escaping."""
import json
import os

print(json.dumps({"content-type": "application/json", "x_iot_auth_token": os.environ["IOT_AUTH_CALLBACK_TOKEN"]}, ensure_ascii=True))
