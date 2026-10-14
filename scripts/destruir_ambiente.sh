#!/usr/bin/env bash
# Remove todo o ambiente (cluster Kind e recursos) criado pelo Terraform.
set -euo pipefail
terraform -chdir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/terraform" destroy -input=false "$@"
