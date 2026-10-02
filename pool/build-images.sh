#!/usr/bin/env bash
# Builds every image an Open Pool deployment runs and tags it for your
# registry. It pushes nothing: it ends by listing the tags and the commands
# to push them.
#
#   pool/build-images.sh                       all seven images
#   pool/build-images.sh openpool agent        only these
#
# Names: orchestrator tunnel openpool agent marketing member admin
#
#   REGISTRY            where the images will live          (tztcloud)
#   VERSION             tag of the pool's own images        (0.1.0)
#   OPENPOOL_SITE_URL   the public site's address           (https://open-pool.com)
#   OPENPOOL_MEMBER_URL the member site's address           (https://app.open-pool.com)
#
# The orchestrator and the tunnel are go-livepeer, built from this branch,
# and are tagged with its version and commit. The public site's pages carry
# the two addresses above, so its image is built for them.
#
# Everything is built from a clean copy of the commit that is checked out,
# not from the working directory: an image is exactly the commit its tag
# names, whatever else is lying in the directory, and go-livepeer does not
# report its version as "dirty".
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if ! git diff --quiet HEAD --; then
  echo "note: changes that are not committed are not in what is built; the images are of commit $(git rev-parse --short=8 HEAD)" >&2
fi
commit="$(git rev-parse --short=8 HEAD)"
clean="$(mktemp -d)"
trap 'rm -rf "$clean"' EXIT
# Images in the repository are stored with Git LFS and no build needs them.
# Copied, not linked: the temporary directory may be on another filesystem.
GIT_LFS_SKIP_SMUDGE=1 git clone --quiet --local --no-hardlinks "$root" "$clean/src"
cd "$clean/src"

REGISTRY="${REGISTRY:-tztcloud}"
VERSION="${VERSION:-0.1.0}"
OPENPOOL_SITE_URL="${OPENPOOL_SITE_URL:-https://open-pool.com}"
OPENPOOL_MEMBER_URL="${OPENPOOL_MEMBER_URL:-https://app.open-pool.com}"
node_version="v$(cat VERSION)-$commit"

declare -A image=(
  [orchestrator]="$REGISTRY/go-livepeer:$node_version"
  [tunnel]="$REGISTRY/livepeer-runner-tunnel:$node_version"
  [openpool]="$REGISTRY/openpool:$VERSION"
  [agent]="$REGISTRY/openpool-agent:$VERSION"
  [marketing]="$REGISTRY/openpool-web-marketing:$VERSION"
  [member]="$REGISTRY/openpool-web-member:$VERSION"
  [admin]="$REGISTRY/openpool-web-admin:$VERSION"
)
order=(orchestrator tunnel openpool agent marketing member admin)
wanted=("${@:-${order[@]}}")

build() {
  case "$1" in
    orchestrator) docker build --build-arg BUILD_TAGS=mainnet,experimental -f docker/Dockerfile -t "${image[$1]}" . ;;
    tunnel)       docker build -f docker/Dockerfile.runner-tunnel -t "${image[$1]}" . ;;
    openpool)     docker build -f pool/docker/Dockerfile -t "${image[$1]}" pool ;;
    agent)        docker build -f pool/docker/Dockerfile.agent --build-arg "VERSION=$VERSION" -t "${image[$1]}" pool ;;
    marketing|member|admin)
      docker build -f pool/web/docker/Dockerfile --build-arg "SITE=$1" \
        --build-arg "OPENPOOL_SITE_URL=$OPENPOOL_SITE_URL" --build-arg "OPENPOOL_MEMBER_URL=$OPENPOOL_MEMBER_URL" \
        -t "${image[$1]}" pool/web ;;
    *) echo "no image is called '$1'; the names are: ${order[*]}" >&2; exit 2 ;;
  esac
}

for name in "${wanted[@]}"; do
  [ -n "${image[$name]:-}" ] || { echo "no image is called '$name'; the names are: ${order[*]}" >&2; exit 2; }
  echo "== $name -> ${image[$name]}"
  build "$name"
done

echo
echo "Built, on this machine only:"
for name in "${wanted[@]}"; do
  printf '  %-13s %s\n' "$name" "${image[$name]}"
done
cat <<NEXT

To publish them:
$(for name in "${wanted[@]}"; do echo "  docker push ${image[$name]}"; done)

The pool service names the tunnel image by digest, which a registry assigns
when the image is pushed. After pushing the tunnel, this prints the value
for TUNNEL_IMAGE in pool/deploy/region/.env:

  docker image inspect --format '{{index .RepoDigests 0}}' ${image[tunnel]}
NEXT
