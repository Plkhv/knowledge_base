#!/usr/bin/env python3
"""Reset or create the `admin` user password using AdminService.

Usage:
  python reset_admin_password.py <new_password>
Or set ADMIN_BOOTSTRAP_PASSWORD environment variable and run without args.

Run this from the `admin/` directory so imports resolve:
  python .\scripts\reset_admin_password.py myNewPassw0rd
"""
import os
import sys
from pathlib import Path

# Ensure parent `admin` package directory is on sys.path when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.admin_service import AdminService


def main():
    new_password = None
    if len(sys.argv) > 1:
        new_password = sys.argv[1]
    else:
        new_password = os.getenv("ADMIN_BOOTSTRAP_PASSWORD") or os.getenv("AIRFLOW_ADMIN_PASSWORD")

    if not new_password:
        print("Error: provide new password as first argument or set ADMIN_BOOTSTRAP_PASSWORD env var")
        return 2

    svc = AdminService()

    # Prefer explicit AIRFLOW_ADMIN_USERNAME if set, else 'admin'
    target_username = os.getenv("AIRFLOW_ADMIN_USERNAME", "admin")

    users = svc.get_all_users()
    admin_user = None
    for u in users:
        if getattr(u, "username", None) == target_username:
            admin_user = u
            break
        if getattr(u, "username", None) == "admin":
            admin_user = u
            break

    if admin_user:
        ok, result = svc.update_user(admin_user.id, password=new_password)
        if ok:
            print(f"Updated password for user '{getattr(admin_user, 'username', admin_user.id)}' (id={admin_user.id})")
            return 0
        else:
            print("Failed to update password:", result)
            return 1
    else:
        ok, res = svc.create_user(
            username=target_username,
            password=new_password,
            full_name="System Administrator",
            role="ADMIN",
            created_by=0,
        )
        if ok:
            print(f"Created admin user '{target_username}' with id {res}")
            return 0
        else:
            print("Failed to create admin user:", res)
            return 1


if __name__ == "__main__":
    sys.exit(main())
