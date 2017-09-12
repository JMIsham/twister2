# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
''' execute.py '''
import contextlib
import os
import subprocess
import tarfile
import tempfile
import traceback

from twister2.tools.cli.src.python.log import Log
from twister2.tools.cli.src.python.result import SimpleResult, ProcessResult, Status
import heron.common.src.python.pex_loader as pex_loader
import heron.tools.cli.src.python.opts as opts
import heron.tools.cli.src.python.jars as jars
import heron.tools.common.src.python.utils.config as config

################################################################################
def heron_class(class_name, lib_jars, extra_jars=None, args=None, java_defines=None):
    '''
    Execute a heron class given the args and the jars needed for class path
    :param class_name:
    :param lib_jars:
    :param extra_jars:
    :param args:
    :param java_defines:
    :return:
    '''
    # default optional params to empty list if not provided
    if extra_jars is None:
        extra_jars = []
    if args is None:
        args = []
    if java_defines is None:
        java_defines = []

    # Format all java -D options that need to be passed while running
    # the class locally.
    java_opts = ['-D' + opt for opt in java_defines]

    # Construct the command line for the sub process to run
    # Because of the way Python execute works,
    # the java opts must be passed as part of the list
    all_args = [config.get_java_path(), "-client", "-Xmx1g"] + \
               java_opts + \
               ["-cp", config.get_classpath(extra_jars + lib_jars)]

    all_args += [class_name] + list(args)

    # set heron_config environment variable
    heron_env = os.environ.copy()
    heron_env['HERON_OPTIONS'] = opts.get_heron_config()

    # print the verbose message
    Log.debug("Invoking class using command: ``%s''", ' '.join(all_args))
    Log.debug("Heron options: {%s}", str(heron_env["HERON_OPTIONS"]))

    # invoke the command with subprocess and print error message, if any
    proc = subprocess.Popen(all_args, env=heron_env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, bufsize=1)
    # stdout message has the information Java program sends back
    # stderr message has extra information, such as debugging message
    return ProcessResult(proc)

def heron_tar(class_name, topology_tar, arguments, tmpdir_root, java_defines):
    '''
    :param class_name:
    :param topology_tar:
    :param arguments:
    :param tmpdir_root:
    :param java_defines:
    :return:
    '''
    # Extract tar to a tmp folder.
    tmpdir = tempfile.mkdtemp(dir=tmpdir_root, prefix='tmp')

    with contextlib.closing(tarfile.open(topology_tar)) as tar:
        tar.extractall(path=tmpdir)

    # A tar generated by pants has all dependency jars under libs/
    # in addition to the topology jar at top level. Pants keeps
    # filename for jar and tar the same except for extension.

    topology_jar = os.path.basename(topology_tar).replace(".tar.gz", "").replace(".tar", "") + ".jar"

    extra_jars = [
        os.path.join(tmpdir, "heron-instance.jar"),
        os.path.join(tmpdir, topology_jar),
        os.path.join(tmpdir, "*"),
        os.path.join(tmpdir, "libs/*")
    ]

    lib_jars = config.get_heron_libs(jars.topology_jars())

    # Now execute the class
    return heron_class(class_name, lib_jars, extra_jars, arguments, java_defines)

