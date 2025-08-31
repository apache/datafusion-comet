import os

for version in ["0.7", "0.8", "0.9"]:
    os.system(f"git clone --depth 1 git@github.com:apache/datafusion-comet.git -b branch-{version} comet-{version}")
    os.system(f"mkdir temp/user-guide/{version}")
    os.system(f"cp -rf comet-{version}/docs/source/user-guide/* temp/user-guide/{version}")