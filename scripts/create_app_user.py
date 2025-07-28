
#!/usr/bin/env python3
import os, sys, argparse
from pathlib import Path
import yaml
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

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

def map_primary_to_hostport(primary_name, servers):
    for s in servers:
        if s.get("rs_host") == primary_name:
            return s["host"], int(s.get("port", 27017))
    host, port = primary_name.split(":")
    return host, int(port)

def main():
    p = argparse.ArgumentParser(description="Create or update an application user")
    p.add_argument("--db", default=os.environ.get("APP_DB", "appdb"))
    p.add_argument("--user", default=os.environ.get("APP_USER", "appuser"))
    p.add_argument("--password", default=os.environ.get("APP_PASS", "appuserpassword"))
    args = p.parse_args()

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

    client, node = try_connect_nodes(servers, admin_user, admin_pass)
    try:
        hello = client.admin.command("hello")
        if not hello.get("isWritablePrimary"):
            primary = hello.get("primary")
            if not primary:
                print("No primary found yet. Try again after election completes.")
                sys.exit(2)
            h, p = map_primary_to_hostport(primary, servers)
            client.close()
            uri = f"mongodb://{admin_user}:{admin_pass}@{h}:{p}/admin?directConnection=true"
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)

        try:
            client[args.db].command("createUser", args.user, pwd=args.password,
                                    roles=[{"role": "readWrite", "db": args.db}])
            print(f"Created user '{args.user}' with readWrite on '{args.db}'.")
        except OperationFailure as e:
            if "already exists" in str(e):
                print(f"User exists; updating password/roles.")
                client[args.db].command("updateUser", args.user, pwd=args.password,
                                        roles=[{"role": "readWrite", "db": args.db}])
                print(f"Updated user '{args.user}'.")
            else:
                raise
    except PyMongoError as e:
        print(f"Error creating app user: {e}")
        sys.exit(1)
    finally:
        try:
            client.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
