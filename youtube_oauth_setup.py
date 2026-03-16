#!/usr/bin/env python3
"""One-time script to obtain a YouTube OAuth2 refresh token.

Run this locally (not in Airflow) to get the refresh token, then store
the outputs as Airflow Variables:

    YOUTUBE_CLIENT_ID      — from Google Cloud Console
    YOUTUBE_CLIENT_SECRET  — from Google Cloud Console
    YOUTUBE_REFRESH_TOKEN  — printed by this script after authorisation

Prerequisites:
    pip install google-auth-oauthlib

Usage:
    python youtube_oauth_setup.py client_secret_*.json

Setup in Google Cloud Console:
    1. Create a project (or reuse one)
    2. Enable the YouTube Data API v3
    3. Create OAuth 2.0 credentials → Desktop app
    4. Download the client_secret_*.json file and pass it as the argument
"""

import argparse
import json
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]


def main():
    parser = argparse.ArgumentParser(description="Generate a YouTube OAuth2 refresh token.")
    parser.add_argument("credentials", help="Path to the client_secret_*.json file downloaded from Google Cloud Console")
    args = parser.parse_args()

    try:
        with open(args.credentials) as f:
            client_config = json.load(f)
    except FileNotFoundError:
        print(f"Error: file not found: {args.credentials}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON in {args.credentials}: {e}", file=sys.stderr)
        sys.exit(1)

    # Support both "installed" and "web" credential types
    config_key = "installed" if "installed" in client_config else "web"
    if config_key not in client_config:
        print("Error: credentials file does not contain 'installed' or 'web' key.", file=sys.stderr)
        sys.exit(1)

    client_id = client_config[config_key]["client_id"]
    client_secret = client_config[config_key]["client_secret"]

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    creds = flow.run_local_server(port=0)

    print("\n" + "=" * 60)
    print("SUCCESS — store these as Airflow Variables:")
    print("=" * 60)
    print(f"YOUTUBE_CLIENT_ID      = {client_id}")
    print(f"YOUTUBE_CLIENT_SECRET  = {client_secret}")
    print(f"YOUTUBE_REFRESH_TOKEN  = {creds.refresh_token}")
    print("=" * 60)
    print("\nAirflow UI: Admin → Variables → + (add each one)")
    print("Or via CLI inside the Airflow pod:")
    print(f'  airflow variables set YOUTUBE_CLIENT_ID "{client_id}"')
    print(f'  airflow variables set YOUTUBE_CLIENT_SECRET "{client_secret}"')
    print(f'  airflow variables set YOUTUBE_REFRESH_TOKEN "{creds.refresh_token}"')


if __name__ == "__main__":
    main()
