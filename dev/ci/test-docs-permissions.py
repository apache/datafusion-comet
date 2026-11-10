# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Test that the docs writer boundary rejects unsafe workflow mutations."""

import importlib.util
from pathlib import Path
import tempfile

spec = importlib.util.spec_from_file_location('ci_config', 'dev/ci/check-ci-config.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
original = Path('.github/workflows/docs.yaml').read_text(encoding='utf-8')
mutations = [
    original.replace('      - name: Copy & push the generated HTML',
                     '      - run: bash site-output/build.sh\n      - name: Copy & push the generated HTML'),
    original.replace('          ref: asf-site', '          ref: main'),
    original.replace('          path: asf-site', '          path: elsewhere', 1),
    original.replace('      contents: read\n', '      contents: read\n      id-token: write\n', 1),
    original.replace('          cd asf-site/\n', '          cd asf-site/\n          bash ../site-output/x.sh\n'),
    original.replace('            /.asf.yaml', '            /docs/'),
    original.replace("            --exclude '/.asf.yaml' \\", "            --exclude '/other' \\"),
    original.replace('          path: site-output', '          path: .'),
    original.replace('          name: comet-docs-site\n          path: site-output',
                     '          name: another-artifact\n          path: site-output'),
    original.replace('          rm -f .asf.yaml .nojekyll',
                     '          bash ./postprocess.sh\n          rm -f .asf.yaml .nojekyll'),
    original.replace('./trusted/.github/actions/download-artifact-retry',
                     './asf-site/.github/actions/download-artifact-retry')
]
with tempfile.TemporaryDirectory() as tmp:
    checker.WORKFLOWS = Path(tmp)
    path = checker.WORKFLOWS / 'docs.yaml'
    path.write_text(original, encoding='utf-8')
    assert checker.check_docs_permission_boundary()
    for mutation in mutations:
        assert mutation != original
        path.write_text(mutation, encoding='utf-8')
        assert not checker.check_docs_permission_boundary()
print('Docs permission boundary mutation checks passed')
