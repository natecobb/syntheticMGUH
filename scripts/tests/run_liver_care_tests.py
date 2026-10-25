"""Run deterministic clinical journey checks in the installed Synthea engine."""
import subprocess
import tempfile
from pathlib import Path

from liver_care_fixtures import ROOT, write_engine_fixtures

if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="liver-care-engine-") as directory:
        folder = Path(directory)
        write_engine_fixtures(folder)
        subprocess.run(["java", "-Xmx2g", "-cp", "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar",
                        "scripts/tests/LiverCareTest.java", str(folder)], cwd=ROOT, check=True)
