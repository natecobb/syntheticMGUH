#!/bin/bash
# Build or copy a stock Synthea 4.0+ fat JAR into ./synthea/bin/
# Usage:
#   ./0_setup_synthea.sh [path_to_synthea_repo_or_git_ref]

set -e

JAR_DEST="synthea/bin/synthea-with-dependencies.jar"
mkdir -p synthea/bin/resources

ARG="${1:-../synthea-4.0.0}"

if [ -d "$ARG" ] && [ -f "$ARG/gradlew" ]; then
    echo "Using local Synthea repository at: $ARG"
    if [ ! -f "$ARG/build/libs/synthea-with-dependencies.jar" ]; then
        echo "Building synthea-with-dependencies.jar..."
        (cd "$ARG" && ./gradlew uberJar -x test -x checkstyleMain -x checkstyleTest)
    fi
    cp "$ARG/build/libs/synthea-with-dependencies.jar" "$JAR_DEST"
else
    REF="${1:-bd8a52ba0}"
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT
    echo "Cloning Synthea ($REF) into temporary build directory..."
    git clone https://github.com/synthetichealth/synthea.git "$TMP_DIR/synthea"
    (cd "$TMP_DIR/synthea" && git checkout "$REF" && ./gradlew uberJar -x test -x checkstyleMain -x checkstyleTest)
    cp "$TMP_DIR/synthea/build/libs/synthea-with-dependencies.jar" "$JAR_DEST"
fi

# PhysiologyValueGenerator uses FileUtils.listFiles on physiology/generators,
# which requires physiology/ to be an unpacked directory on the classpath.
unzip -q -o "$JAR_DEST" "physiology/*" -d synthea/bin/resources

echo "✓ Synthea runtime ready at: $JAR_DEST ($(du -h "$JAR_DEST" | cut -f1))"
