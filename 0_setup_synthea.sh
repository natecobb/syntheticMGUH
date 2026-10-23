#!/bin/bash
# Build or copy Synthea fat JAR (synthea-with-dependencies.jar) into ./bin/
# Usage:
#   ./0_setup_synthea.sh [path_to_synthea_repo_or_git_ref]

set -e

JAR_DEST="bin/synthea-with-dependencies.jar"
mkdir -p bin bin/resources

ARG="${1:-../synthea-4.0.0}"

if [ -d "$ARG" ] && [ -f "$ARG/gradlew" ]; then
    echo "Using local Synthea repository at: $ARG"
    if [ ! -f "$ARG/build/libs/synthea-with-dependencies.jar" ]; then
        echo "Building synthea-with-dependencies.jar..."
        (cd "$ARG" && ./gradlew uberJar -x test -x checkstyleMain -x checkstyleTest)
    fi
    cp "$ARG/build/libs/synthea-with-dependencies.jar" "$JAR_DEST"
else
    REF="${1:-master}"
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT
    echo "Cloning Synthea ($REF) into temporary build directory..."
    git clone --depth 1 --branch "$REF" https://github.com/synthetichealth/synthea.git "$TMP_DIR/synthea"
    echo "Building synthea-with-dependencies.jar..."
    (cd "$TMP_DIR/synthea" && ./gradlew uberJar -x test -x checkstyleMain -x checkstyleTest)
    cp "$TMP_DIR/synthea/build/libs/synthea-with-dependencies.jar" "$JAR_DEST"
fi

# Ensure no custom providers/syntheticMGUH/ files are baked inside the JAR
# (otherwise classpath lookup in Utilities.readResource shadows ./providers/syntheticMGUH/ on disk)
zip -d "$JAR_DEST" "providers/syntheticMGUH/*" >/dev/null 2>&1 || true

# PhysiologyValueGenerator uses FileUtils.listFiles on physiology/generators,
# which requires physiology/ to be an unpacked directory on the classpath.
unzip -q -o "$JAR_DEST" "physiology/*" -d bin/resources

echo "✓ Synthea JAR ready at: $JAR_DEST ($(du -h "$JAR_DEST" | cut -f1))"
