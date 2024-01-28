<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Comet Debugging Guide

This HOWTO describes how to debug JVM code and Native code concurrently. The guide assumes you have:
1. Intellij as the Java IDE
2. CLion as the Native IDE. For Rust code, the CLion Rust language plugin is required. Note that the
Intellij Rust plugin is not sufficient.
3. CLion/LLDB as the native debugger. CLion ships with a bundled LLDB and the Rust community has
its own packaging of LLDB (`lldb-rust`). Both provide a better display of Rust symbols than plain
LLDB or the LLDB that is bundled with XCode. We will use the LLDB packaged with CLion for this guide.
4. We will use a Comet _unit_ test as the canonical use case.

_Caveat: The steps here have only been tested with JDK 11_ on Mac (M1)

## Debugging for Advanced Developers

Add a `.lldbinit` to comet/core. This is not strictly necessary but will be useful if you want to
use advanced `lldb` debugging.

### In Intellij

1. Set a breakpoint in `NativeBase.load()`, at a point _after_ the Comet library has been loaded.

1. Add a Debug Configuration for the unit test

1. In the Debug Configuration for that unit test add `-Xint` as a JVM parameter. This option is
undocumented *magic*. Without this, the LLDB debugger hits a EXC_BAD_ACCESS (or EXC_BAD_INSTRUCTION) from
which one cannot recover.

1. Add  a println to the unit test to print the PID of the JVM process. (jps can also be used but this is less error prone if you have multiple jvm processes running)
     ``` JDK8
          println("Waiting for Debugger: PID - ", ManagementFactory.getRuntimeMXBean().getName())
     ```
      This will print something like : `PID@your_machine_name`.

     For JDK9 and newer
     ```JDK9
          println("Waiting for Debugger: PID - ", ProcessHandle.current.pid)
     ```

     ==> Note the PID

1. Debug-run the test in Intellij and wait for the breakpoint to be hit

### In CLion

1. After the breakpoint is hit in Intellij, in Clion (or LLDB from terminal or editor) -

   1. Attach to the jvm process (make sure the PID matches). In CLion, this is `Run -> Atttach to process`

   1. Put your breakpoint in the native code

1. Go back to intellij and resume the process.

1. Most debugging in CLion is similar to Intellij. For advanced LLDB based debugging the LLDB command line can be accessed from the LLDB tab in the Debugger view. Refer to the [LLDB manual](https://lldb.llvm.org/use/tutorial.html) for LLDB commands.

### After your debugging is done,

1. In CLion, detach from the process if not already detached

2. In Intellij, the debugger might have lost track of the process. If so, the debugger tab
   will show the process as running (even if the test/job is shown as completed).

3. Close the debugger tab, and if the IDS asks whether it should terminate the process,
   click Yes.

4. In terminal, use jps to identify the process with the process id you were debugging. If
   it shows up as running, kill -9 [pid]. If that doesn't remove the process, don't bother,
   the process will be left behind as a zombie and will consume no (significant) resources.
   Eventually it will be cleaned up when you reboot possibly after a software update.

### Additional Info

OpenJDK mailing list on debugging the JDK on MacOS
https://mail.openjdk.org/pipermail/hotspot-dev/2019-September/039429.html

Detecting the debugger
https://stackoverflow.com/questions/5393403/can-a-java-application-detect-that-a-debugger-is-attached#:~:text=No.,to%20let%20your%20app%20continue.&text=I%20know%20that%20those%20are,meant%20with%20my%20first%20phrase).
