
#!/usr/bin/env python3
import os, sys
from pathlib import Path
import yaml
from pymongo import MongoClient
from pymongo.errors import PyMongoError

CONFIG_FILE = Path(__file__).with_name("mongo_servers.yml")

def load_config(path):
    with path.open() as f:
        return yaml.safe_load(f)

def try_connect_nodes(servers, admin_user, admin_pass):
    last_err = None
    for s in servers:
        uri = f"mongodb://{admin_user}:{admin_pass}@{s['host']}:{s.get('port',27017)}/admin?directConnection=true"
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client, s
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Unable to connect to any node directly. Last error: {last_err}")

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

    try:
        client, node = try_connect_nodes(servers, admin_user, admin_pass)
        status = client.admin.command("replSetGetStatus")
        set_name = status.get('set', '(unknown)')
        print(f"Replica set: {set_name}  (queried via {node['host']}:{node.get('port',27017)})")
        members = status.get('members', [])
        primary = next((m for m in members if m.get('stateStr') == 'PRIMARY'), None)
        if primary:
            print(f"PRIMARY: {primary.get('name')}")
        else:
            print("PRIMARY: (none yet)")
        print("-" * 60)
        unhealthy = False
        for m in members:
            name = m.get('name')
            state = m.get('stateStr')
            health = 'UP' if m.get('health', 0) == 1 else 'DOWN'
            sync = m.get('syncingTo') or ''
            print(f"{name:<25} state={state:<10} health={health:<4} optime={m.get('optimeDate','')} syncingTo={sync}")
            if state not in ('PRIMARY', 'SECONDARY') or m.get('health', 0) != 1:
                unhealthy = True
        print("-" * 60)
        if unhealthy or not primary:
            print("Replica set is not fully healthy.")
            sys.exit(2)
        print("Replica set looks healthy.")
    except PyMongoError as e:
        print(f"Error getting status: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        try:
            client.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
