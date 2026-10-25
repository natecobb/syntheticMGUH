"""Execute the integrated cirrhosis parent in the stock engine."""
import subprocess
import tempfile
from pathlib import Path

from cirrhosis_integration_fixtures import ROOT, write_all

if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="cirrhosis-integration-") as directory:
        folder = Path(directory)
        write_all(folder)
        subprocess.run(["java", "-Xmx2g", "-cp", "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar",
                        "scripts/tests/CirrhosisIntegrationTest.java", str(folder)], cwd=ROOT, check=True)
