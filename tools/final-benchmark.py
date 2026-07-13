#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

BASE = os.environ.get(
    "HENOSIS_CORE_URL", "http://127.0.0.1:4481/henosis.v1.GraphService/"
)
ROOT = Path(os.environ.get("HENOSIS_ROOT", Path(__file__).resolve().parents[3]))
STATE = Path(os.environ.get("HENOSIS_BENCHMARK_STATE", "/tmp/henosis-demo-state.json"))
EVIDENCE = Path(os.environ.get("HENOSIS_BENCHMARK_EVIDENCE", "/tmp/henosis-demo-evidence"))
EVIDENCE.mkdir(exist_ok=True)
ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz"


def b64(value):
    return base64.b64encode(value).decode()


def json_bytes(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def uuid7():
    milliseconds = int(time.time() * 1000)
    value = bytearray(milliseconds.to_bytes(6, "big") + secrets.token_bytes(10))
    value[6] = (value[6] & 0x0F) | 0x70
    value[8] = (value[8] & 0x3F) | 0x80
    return bytes(value)


def typeid(prefix, raw):
    number = int.from_bytes(raw, "big")
    suffix = "".join(ALPHABET[(number >> (5 * (25 - index))) & 31] for index in range(26))
    return f"{prefix}_{suffix}"


def call(method, payload):
    request = urllib.request.Request(
        BASE + method,
        data=json_bytes(payload),
        method="POST",
        headers={
            "Authorization": "Bearer core-dev-token",
            "Connect-Protocol-Version": "1",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.loads(response.read())
    except urllib.error.HTTPError as error:
        print(error.read().decode(), file=sys.stderr)
        raise
    evidence = EVIDENCE / f"{int(time.time() * 1000)}-{method}.json"
    evidence.write_text(json.dumps(result, indent=2) + "\n")
    return result


def run_json(command, cwd):
    return json.loads(subprocess.check_output(command, cwd=cwd, text=True))


def definition(path):
    tsx = ROOT / "repos/platform/node_modules/.bin/tsx"
    return run_json([str(tsx), str(path)], path.parent)


def derive_database(environment):
    component = run_json(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "henosis-supabase-derive",
            "--",
            str(ROOT / "repos/service-d"),
        ],
        ROOT / "repos/connector-supabase",
    )
    schema = "benchmark_" + environment[-10:].replace("-", "_")
    context = component["connectorContext"]
    context["resourceId"] = schema
    context["target"]["schema"] = schema
    for migration in context["migrations"]:
        migration["sql"] = migration["sql"].replace("service_d", schema)
        migration["checksum"] = "sha256:" + hashlib.sha256(
            migration["sql"].encode()
        ).hexdigest()
    return component


def derive_worker(service, environment, dependencies):
    repository = ROOT / f"repos/{service}"
    authored = definition(repository / "henosis.ts")
    component = run_json(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "henosis-cloudflare-derive-component",
            "--",
            str(repository),
            environment,
        ],
        ROOT / "repos/connector-cloudflare",
    )
    slots = []
    hashes = []
    for key, reference in authored["inputs"].items():
        digest = dependencies[reference["component"]]
        encoded = list(bytes.fromhex(digest))
        slots.append(
            {
                "key": key,
                "producer": reference["component"],
                "output": reference["output"],
                "producerSpecHash": encoded,
            }
        )
        if encoded not in hashes:
            hashes.append(encoded)
    component["dependsOn"] = hashes
    component["connectorContext"]["slots"] = sorted(
        slots, key=lambda value: value["key"]
    )
    return component


def derive_tunnel(environment):
    authored = definition(ROOT / "repos/service-d/tunnel.henosis.ts")
    return run_json(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "henosis-cloudflare-derive-component",
            "--",
            "--tunnel",
            authored["name"],
            environment,
            authored["origin"]["hostname"],
            str(authored["origin"]["port"]),
        ],
        ROOT / "repos/connector-cloudflare",
    )


def register(component):
    dependencies = [
        b64(bytes(value)) if isinstance(value, list) else value
        for value in component["dependsOn"]
    ]
    spec = {
        "name": component["name"],
        "connector": component["connector"],
        "outputsSchema": b64(json_bytes(component["outputsSchema"])),
        "dependsOn": dependencies,
        "connectorContext": b64(json_bytes(component["connectorContext"])),
    }
    result = call("RegisterComponentSpec", {"spec": spec})
    encoded = result["component"]["hash"]
    return encoded, base64.b64decode(encoded).hex()


def create():
    raw = uuid7()
    environment = typeid("preview", raw)
    database_b64, database_hex = register(derive_database(environment))
    tunnel_b64, _ = register(derive_tunnel(environment))
    backend_b64, backend_hex = register(
        derive_worker("service-e", environment, {"service-d": database_hex})
    )
    frontend_b64, _ = register(
        derive_worker("service-f", environment, {"service-e": backend_hex})
    )
    result = call(
        "CreateGraph",
        {
            "graphId": b64(raw),
            "componentSpecHashes": [
                database_b64,
                tunnel_b64,
                backend_b64,
                frontend_b64,
            ],
            "requestId": b64(uuid7()),
        },
    )
    state = {
        "graphId": b64(raw),
        "environment": environment,
        "generation": int(result["graph"]["generation"]),
        "specs": {
            "service-d": database_b64,
            "supabase-private": tunnel_b64,
            "service-e": backend_b64,
            "service-f": frontend_b64,
        },
    }
    STATE.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(state, indent=2))


def update():
    state = json.loads(STATE.read_text())
    database_b64, database_hex = register(derive_database(state["environment"]))
    backend_b64, backend_hex = register(
        derive_worker("service-e", state["environment"], {"service-d": database_hex})
    )
    frontend_b64, _ = register(
        derive_worker("service-f", state["environment"], {"service-e": backend_hex})
    )
    replacements = [
        {"currentSpecHash": current, "replacementSpecHash": replacement}
        for current, replacement in [
            (state["specs"]["service-d"], database_b64),
            (state["specs"]["service-e"], backend_b64),
            (state["specs"]["service-f"], frontend_b64),
        ]
        if current != replacement
    ]
    result = call(
        "UpdateComponents",
        {
            "graphId": state["graphId"],
            "expectedGeneration": str(state["generation"]),
            "replacements": replacements,
            "requestId": b64(uuid7()),
        },
    )
    state["generation"] = int(result["graph"]["generation"])
    state["specs"].update(
        {
            "service-d": database_b64,
            "service-e": backend_b64,
            "service-f": frontend_b64,
        }
    )
    STATE.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(state, indent=2))


def add_failure():
    state = json.loads(STATE.read_text())
    component = derive_database(state["environment"] + "_bad")
    component["name"] = "service-d-bad"
    context = component["connectorContext"]
    context["resourceId"] = "service_d_bad"
    context["target"]["schema"] = "service_d_bad"
    migration = context["migrations"][0]
    migration["id"] = "20260713060000_bad_destructive_demo"
    migration["sql"] = (
        "create schema if not exists service_d_bad;\n"
        "create table service_d_bad.items (id bigint primary key);\n"
        "drop table service_d_bad.items;\n"
    )
    migration["checksum"] = "sha256:" + hashlib.sha256(
        migration["sql"].encode()
    ).hexdigest()
    bad_b64, _ = register(component)
    result = call(
        "AddComponents",
        {
            "graphId": state["graphId"],
            "expectedGeneration": str(state["generation"]),
            "componentSpecHashes": [bad_b64],
            "requestId": b64(uuid7()),
        },
    )
    state["generation"] = int(result["graph"]["generation"])
    state["specs"]["service-d-bad"] = bad_b64
    STATE.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(state, indent=2))


def provision_temporary():
    config = Path("/tmp/henosis-wrangler-temporary")
    shutil.rmtree(config, ignore_errors=True)
    config.mkdir()
    environment = os.environ.copy()
    environment.update({"HOME": str(config), "XDG_CONFIG_HOME": str(config)})
    subprocess.run(
        [
            "npx",
            "-y",
            "wrangler@4.110.0",
            "deploy",
            "--temporary",
            "--cwd",
            str(ROOT / "repos/service-e"),
        ],
        check=True,
        env=environment,
    )
    credentials = tomllib.loads(
        (config / ".wrangler/wrangler-temporary-account.toml").read_text()
    )
    values = {
        "CLOUDFLARE_ACCOUNT_ID": credentials["account"]["id"],
        "CLOUDFLARE_API_TOKEN": credentials["account"]["apiToken"],
        "CLOUDFLARE_ACCOUNT_SUBDOMAIN": credentials["account"]["name"]
        .lower()
        .replace(" ", "-"),
    }
    path = ROOT / "infra/.env"
    lines = path.read_text().splitlines() if path.exists() else []
    output = []
    seen = set()
    for line in lines:
        key = line.split("=", 1)[0] if "=" in line else ""
        if key in values:
            output.append(f"{key}={values[key]}")
            seen.add(key)
        else:
            output.append(line)
    output.extend(f"{key}={value}" for key, value in values.items() if key not in seen)
    path.write_text("\n".join(output) + "\n")
    path.chmod(0o600)
    print(credentials["claim"]["url"])


def get():
    state = json.loads(STATE.read_text())
    print(json.dumps(call("GetGraph", {"graphId": state["graphId"]}), indent=2))


if __name__ == "__main__":
    actions = {
        "provision-temporary": provision_temporary,
        "create": create,
        "update": update,
        "add-failure": add_failure,
        "get": get,
    }
    actions[sys.argv[1]]()
