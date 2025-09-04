import sys
from pathlib import Path

def file_to_class_name(path: Path) -> str | None:
    parts = path.parts
    if "org" not in parts or "apache" not in parts:
        return None
    org_index = parts.index("org")
    package_parts = parts[org_index:]
    class_name = ".".join(package_parts)
    class_name = class_name.replace(".scala", "")
    return class_name

if __name__ == "__main__":

    # ignore traits, abstract classes, and intentionally skipped test suites
    ignore_list = [
        "org.apache.comet.parquet.ParquetReadSuite",
        "org.apache.comet.parquet.ParquetReadFromS3Suite",
        "org.apache.spark.sql.comet.CometPlanStabilitySuite",
        "org.apache.spark.sql.comet.ParquetDatetimeRebaseSuite"
    ]

    for workflow_filename in [".github/workflows/pr_build_linux.yml", ".github/workflows/pr_build_macos.yml"]:
        workflow = open(workflow_filename, encoding="utf-8").read()

        root = Path(".")
        for path in root.rglob("*Suite.scala"):
            class_name = file_to_class_name(path)
            if class_name:
                if "Shim" in class_name:
                    continue
                if class_name in ignore_list:
                    continue
                if class_name not in workflow:
                    print(f"Suite not found in workflow {workflow_filename}: {class_name}")
                    sys.exit(-1)
                print(f"Found {class_name} in {workflow_filename}")
