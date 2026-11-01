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

"""Exercise the docs deploy copy without checking out or pushing the site."""

import os
from pathlib import Path
import subprocess
import tempfile

workflow = Path('.github/workflows/docs.yaml').read_text(encoding='utf-8')
script = workflow.split('      - name: Copy & push the generated HTML\n', 1)[1]
script = script.split('        run: |\n', 1)[1].split('          git status --porcelain', 1)[0]
script = '\n'.join(line[10:] for line in script.splitlines())

with tempfile.TemporaryDirectory() as tmp:
    fixture = Path(tmp)
    site = fixture / 'asf-site'
    output = fixture / 'site-output'
    (site / '.git').mkdir(parents=True)
    output.mkdir()
    (site / '.git' / 'sentinel').write_text('git metadata')
    (site / 'old-page').write_text('stale')
    old_asf = site / '.asf.yaml'
    new_asf = output / '.asf.yaml'
    old_asf.write_text('old metadata')
    new_asf.write_text('new metadata')
    # rsync's quick check can otherwise miss different, same-sized files with
    # identical mtimes. The explicit cp must still update the ASF metadata.
    os.utime(old_asf, (1000000000, 1000000000))
    os.utime(new_asf, (1000000000, 1000000000))
    (output / 'index.html').write_text('new page')
    (output / '.nojekyll').touch()
    subprocess.run(['bash', '-e', '-c', script], cwd=fixture, check=True)
    assert (site / '.git' / 'sentinel').read_text() == 'git metadata'
    assert old_asf.read_text() == 'new metadata'
    assert (site / '.nojekyll').exists()
    assert (site / 'index.html').read_text() == 'new page'
    assert not (site / 'old-page').exists()

print('Docs site copy check passed')
