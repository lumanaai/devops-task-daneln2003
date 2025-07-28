
#!/usr/bin/env python3
import os, sys, time
from pathlib import Path
import yaml
from pymongo import MongoClient
from pymongo.errors import PyMongoError

CONFIG_FILE = Path(__file__).with_name("mongo_servers.yml")
RS_NAME = os.environ.get("RS_NAME", "rs0")

def load_config(path: Path):
    with path.open() as f:
        return yaml.safe_load(f)

def seed_list(servers):
    return ",".join([f"{s['host']}:{s.get('port', 27017)}" for s in servers])

def derive_rs_hosts(servers):
    hosts = []
    for s in servers:
        rh = s.get("rs_host")
        if rh:
            hosts.append(rh)
        else:
            hosts.append(f"{s['host']}:{s.get('port',27017)}")
    return hosts

def connect(uri):
    return MongoClient(uri, serverSelectionTimeoutMS=5000)

def already_initialized(client):
    try:
        hello = client.admin.command("hello")
    except PyMongoError:
        hello = client.admin.command("isMaster")
    return bool(hello.get("setName"))

def main():
    cfg = load_config(CONFIG_FILE)
    servers = cfg.get("servers", [])
    if not servers:
        print("No servers in config.")
        sys.exit(1)
    admin_user = (cfg.get("admin_user") or "root")
    admin_pass = cfg.get("admin_password") or os.environ.get("MONGO_ROOT_PASSWORD")
    if not admin_pass:
        print("Missing admin password. Set admin_password in mongo_servers.yml or export MONGO_ROOT_PASSWORD.")
        sys.exit(1)

    # Smoke test: direct ping to each node (no replicaSet param, directConnection)
    for s in servers:
        uri = f"mongodb://{admin_user}:{admin_pass}@{s['host']}:{s.get('port',27017)}/admin?directConnection=true"
        try:
            c = connect(uri)
            c.admin.command("ping")
            print(f"OK: {s['host']}:{s.get('port',27017)}")
        except Exception as e:
            print(f"ERROR: cannot reach {s['host']}:{s.get('port',27017)} as {admin_user}: {e}")
            sys.exit(1)
        finally:
            try:
                c.close()
            except Exception:
                pass

    # Connect to the first node to run rs.initiate if needed
    first = servers[0]
    uri0 = f"mongodb://{admin_user}:{admin_pass}@{first['host']}:{first.get('port',27017)}/admin?directConnection=true"
    client0 = connect(uri0)
    try:
        if already_initialized(client0):
            print(f"Replica set '{RS_NAME}' already initialized; skipping rs.initiate().")
            return

        members = [{"_id": i, "host": h} for i, h in enumerate(derive_rs_hosts(servers))]
        cfg_doc = {"_id": RS_NAME, "members": members}
        print("Initializing replica set with config:", cfg_doc)
        client0.admin.command("replSetInitiate", cfg_doc)
        print("rs.initiate() sent; waiting for PRIMARY election...")
        # Poll for PRIMARY
        deadline = time.time() + 60
        last_err = None
        while time.time() < deadline:
            try:
                st = client0.admin.command("replSetGetStatus")
                primary = next((m for m in st.get("members", []) if m.get("stateStr") == "PRIMARY"), None)
                if primary:
                    print("PRIMARY:", primary.get("name"))
                    return
            except Exception as e:
                last_err = e
            time.sleep(1)
        print("Warning: timeout waiting for PRIMARY election.", ("Last error: %s" % last_err) if last_err else "")
    finally:
        client0.close()

if __name__ == "__main__":
    main()
