/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.internal.SQLConf

class CometConfSuite extends AnyFunSuite {

  test("primary key wins over alternative when both are set") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.primaryWins")
      .withAlternative("spark.comet.testing.alias.primaryWins.old")
      .category("testing")
      .booleanConf
      .createWithDefault(false)

    val conf = new SQLConf
    conf.setConfString(entry.key, "true")
    conf.setConfString(entry.alternatives.head, "false")

    assert(entry.get(conf))
  }

  test("alternative is read when primary key is unset, with expected value") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.readsAlternative")
      .withAlternative("spark.comet.testing.alias.readsAlternative.old")
      .category("testing")
      .intConf
      .createWithDefault(0)

    val conf = new SQLConf
    conf.setConfString(entry.alternatives.head, "42")

    assert(entry.get(conf) == 42)
  }

  test("default is returned when neither primary nor alternative is set") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.defaultOnly")
      .withAlternative("spark.comet.testing.alias.defaultOnly.old")
      .category("testing")
      .booleanConf
      .createWithDefault(true)

    val conf = new SQLConf
    assert(entry.get(conf))
  }

  test("multiple alternatives are checked in the order provided") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.multi")
      .withAlternative(
        "spark.comet.testing.alias.multi.older",
        "spark.comet.testing.alias.multi.oldest")
      .category("testing")
      .intConf
      .createWithDefault(0)

    val conf = new SQLConf
    conf.setConfString(entry.alternatives.head, "1")
    conf.setConfString(entry.alternatives(1), "2")

    // First alternative wins, not the second.
    assert(entry.get(conf) == 1)
  }

  test("OptionalConfigEntry reads through an alternative") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.optional")
      .withAlternative("spark.comet.testing.alias.optional.old")
      .category("testing")
      .stringConf
      .createOptional

    val conf = new SQLConf
    assert(entry.get(conf).isEmpty)

    conf.setConfString(entry.alternatives.head, "hello")
    assert(entry.get(conf).contains("hello"))
  }

  test("value from an alternative goes through the type converter") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.typed")
      .withAlternative("spark.comet.testing.alias.typed.old")
      .category("testing")
      .booleanConf
      .createWithDefault(false)

    val conf = new SQLConf
    conf.setConfString(entry.alternatives.head, "TRUE")

    // The boolean converter accepts case-insensitive "TRUE"/"FALSE"; if the alternative
    // were returned raw, this assertion would fail.
    assert(entry.get(conf))
  }

  test("COMET_FORCE_SHJ reads the deprecated replaceSortMergeJoin key as an alias") {
    val conf = new SQLConf
    conf.setConfString(s"${CometConf.COMET_EXEC_CONFIG_PREFIX}.replaceSortMergeJoin", "true")

    assert(CometConf.COMET_FORCE_SHJ.get(conf))
  }

  test("COMET_SHUFFLE_ENABLED reads the deprecated exec.shuffle.enabled key as an alias") {
    val conf = new SQLConf
    conf.setConfString(s"${CometConf.COMET_EXEC_CONFIG_PREFIX}.shuffle.enabled", "false")

    assert(!CometConf.COMET_SHUFFLE_ENABLED.get(conf))
  }

  test("COMET_SHUFFLE_JVM_SPILL_THRESHOLD reads the deprecated dots-in-segment key") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.columnar.shuffle.spill.threshold", "12345")

    assert(CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.get(conf) == 12345)
  }

  test("COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO reads the deprecated top-level key") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.shuffle.preferDictionary.ratio", "3.5")

    assert(CometConf.COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO.get(conf) == 3.5)
  }

  test("COMET_EXPLAIN_CODEGEN_ENABLED reads deprecated explainCodegen.enabled as an alias") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.explainCodegen.enabled", "true")

    assert(CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.get(conf))
  }

  test("COMET_EXPLAIN_FALLBACK_ENABLED reads deprecated explainFallback.enabled as an alias") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.explainFallback.enabled", "true")

    assert(CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get(conf))
  }

  test("native write flags share the spark.comet.write namespace") {
    Seq(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED,
      CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED,
      CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED).foreach { entry =>
      assert(entry.key.startsWith("spark.comet.write."), entry.key)
    }
  }

  test("remote shuffle frame and admission limits have bounded defaults") {
    val conf = new SQLConf

    assert(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.get(conf) == 64L * 1024 * 1024)
    assert(CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES.get(conf) == 512L * 1024 * 1024)
  }

  test("remote shuffle frame limit rejects incomplete frames and oversized JVM requests") {
    val conf = new SQLConf
    val entry = CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES

    conf.setConfString(entry.key, "19b")
    assertThrows[IllegalArgumentException](entry.get(conf))

    conf.setConfString(entry.key, s"${Int.MaxValue - 15}b")
    assertThrows[IllegalArgumentException](entry.get(conf))

    conf.setConfString(entry.key, "20b")
    assert(entry.get(conf) == 20)
  }

  test("remote shuffle admission limit reserves complete native, JNI, and client frames") {
    val conf = new SQLConf
    val entry = CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES

    conf.setConfString(entry.key, "75b")
    assertThrows[IllegalArgumentException](entry.get(conf))

    conf.setConfString(entry.key, s"${Int.MaxValue.toLong + 1}b")
    assertThrows[IllegalArgumentException](entry.get(conf))

    conf.setConfString(entry.key, "76b")
    assert(entry.get(conf) == 76)
  }

  test("JVM shuffle batch size must be positive") {
    val conf = new SQLConf
    val entry = CometConf.COMET_SHUFFLE_JVM_BATCH_SIZE

    // A batch size of 0 never advances the native loop that writes sorted spill files.
    Seq("0", "-1").foreach { v =>
      conf.setConfString(entry.key, v)
      assertThrows[IllegalArgumentException](entry.get(conf))
    }

    conf.setConfString(entry.key, "1")
    assert(entry.get(conf) == 1)
  }

  test("CometConf initializes when spark.comet.batchSize is below the JVM shuffle batch size") {
    // Defines its own copy of every org.apache.comet class, so that loading CometConf through it
    // runs CometConf's initializer again. Everything else comes from the parent.
    val loader = new ClassLoader(getClass.getClassLoader) {
      override def loadClass(name: String, resolve: Boolean): Class[_] = {
        if (!name.startsWith("org.apache.comet.")) {
          super.loadClass(name, resolve)
        } else {
          getClassLoadingLock(name).synchronized {
            Option(findLoadedClass(name)).getOrElse {
              val in = getParent.getResourceAsStream(name.replace('.', '/') + ".class")
              if (in == null) throw new ClassNotFoundException(name)
              val bytes =
                try in.readAllBytes()
                finally in.close()
              defineClass(name, bytes, 0, bytes.length)
            }
          }
        }
      }
    }

    // An executor first loads CometConf inside a task, where SQLConf.get holds the session's
    // confs. The initializer must not depend on them.
    val conf = new SQLConf
    conf.setConfString(CometConf.COMET_BATCH_SIZE.key, "4096")
    val cometConfClass =
      try {
        SQLConf.withExistingConf(conf) {
          // scalastyle:off classforname
          Class.forName(CometConf.getClass.getName, true, loader)
          // scalastyle:on classforname
        }
      } catch {
        // ScalaTest aborts the whole run on this error, so report it as a failure instead.
        case e: ExceptionInInitializerError => fail("CometConf failed to initialize", e.getCause)
      }
    assert(cometConfClass ne CometConf.getClass)
  }

  test("JVM shuffle batch size is capped at spark.comet.batchSize where it is read") {
    val conf = new SQLConf
    val entry = CometConf.COMET_SHUFFLE_JVM_BATCH_SIZE
    conf.setConfString(CometConf.COMET_BATCH_SIZE.key, "4096")

    // The JVM shuffle writers read the conf that is current on the task thread. The default is
    // capped there, while the entry itself keeps it.
    SQLConf.withExistingConf(conf) {
      assert(entry.get() == 8192)
      assert(CometConf.jvmShuffleBatchSize() == 4096)
    }

    // A larger value set explicitly is capped too, and a smaller one is used as it is.
    conf.setConfString(entry.key, "16384")
    assert(CometConf.jvmShuffleBatchSize(conf) == 4096)
    conf.setConfString(entry.key, "1024")
    assert(CometConf.jvmShuffleBatchSize(conf) == 1024)
  }

  test("memory pool type is accepted in any case and lowercased") {
    val conf = new SQLConf
    val entry = CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE

    conf.setConfString(entry.key, "Greedy_Unified")
    assert(entry.get(conf) == "greedy_unified")

    conf.setConfString(entry.key, "FAIR_UNIFIED")
    assert(entry.get(conf) == "fair_unified")

    conf.setConfString(entry.key, "fair")
    val e = intercept[IllegalArgumentException](entry.get(conf))
    assert(e.getMessage.contains("fair_unified, greedy_unified"))
  }

  test(
    "COMET_EXPLAIN_FALLBACK_LOG_ENABLED reads deprecated logFallbackReasons.enabled as alias") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.logFallbackReasons.enabled", "true")

    assert(CometConf.COMET_EXPLAIN_FALLBACK_LOG_ENABLED.get(conf))
  }
}
