#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repo_root"

# Scrub ambient AWS auth so local runs match CI and don't mask missing test setup.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_REGION AWS_DEFAULT_REGION
unset AWS_SHARED_CREDENTIALS_FILE AWS_CONFIG_FILE
unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME
export AWS_EC2_METADATA_DISABLED=true

go build -v ./...
go test -v ./...
