#!/usr/bin/env bash
# OSS Exit Gate Release Script for D11: GoogleCloudPlatform/cloud-for-marketing (cloud-for-marketing/npm — b/576685161, b/576678670)
set -euo pipefail

EXIT_GATE_AR_URL="https://us-npm.pkg.dev/oss-exit-gate-prod/cloud-for-marketing--npm/"
EXIT_GATE_MANIFEST_GCS="gs://oss-exit-gate-prod-projects-bucket/cloud-for-marketing/npm/manifests"

PKG_DIRS=(
  "marketing-analytics/activation/common-libs/nodejs-common"
  "marketing-analytics/activation/data-tasks-coordinator"
  "marketing-analytics/activation/gmp-googleads-connector"
  "marketing-analytics/activation/sheets-based-installer"
)

ACCESS_TOKEN="$(gcloud auth print-access-token)"
NPMRC_TMP="$(mktemp)"
cat << EOF > "${NPMRC_TMP}"
registry=${EXIT_GATE_AR_URL}
//us-npm.pkg.dev/oss-exit-gate-prod/cloud-for-marketing--npm/:always-auth=true
//us-npm.pkg.dev/oss-exit-gate-prod/cloud-for-marketing--npm/:_authToken=${ACCESS_TOKEN}
EOF
trap 'rm -f "${NPMRC_TMP}"' EXIT

PACKAGES_JSON="[]"
for dir in "${PKG_DIRS[@]}"; do
  NAME="$(jq -r .name "${dir}/package.json")"
  VERSION="$(jq -r .version "${dir}/package.json")"
  if npm view "${NAME}@${VERSION}" version --registry https://registry.npmjs.org >/dev/null 2>&1; then
    echo "Skipping ${NAME}@${VERSION} (already published on npmjs.org)"
    continue
  fi
  SHORT_NAME="${NAME#@google-cloud/}"
  echo "Staging ${NAME}@${VERSION} from ${dir} to ${EXIT_GATE_AR_URL}..."
  (cd "${dir}" && npm publish --userconfig="${NPMRC_TMP}" --registry="${EXIT_GATE_AR_URL}")
  PACKAGES_JSON="$(echo "${PACKAGES_JSON}" | jq --arg n "${SHORT_NAME}" --arg v "${VERSION}" '. + [{name: $n, version: $v}]')"
done

if [[ "$(echo "${PACKAGES_JSON}" | jq 'length')" -eq 0 ]]; then
  echo "All package versions are already published on npmjs.org. Nothing to release."
  exit 0
fi

MANIFEST_FILE="$(mktemp)"
jq -n --argjson pkgs "${PACKAGES_JSON}" '{
  publish_all: false,
  publishing_groups: [
    {
      namespace: "@google-cloud",
      packages: $pkgs
    }
  ]
}' > "${MANIFEST_FILE}"

gcloud storage cp "${MANIFEST_FILE}" \
  "${EXIT_GATE_MANIFEST_GCS}/manifest-$(date +%Y%m%d-%H%M%S).json"
rm -f "${MANIFEST_FILE}"
echo "Done! OSS Exit Gate release triggered for cloud-for-marketing/npm."
