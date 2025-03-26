// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Common Parquet errors and macros.

use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use jni::errors::{Exception, ToException};
use regex::Regex;

use std::{
    any::Any,
    convert,
    fmt::Write,
    panic::{catch_unwind, UnwindSafe},
    result, str,
    str::Utf8Error,
    sync::{Arc, Mutex},
};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jboolean, jbyte, jchar, jdouble, jfloat, jint, jlong, jobject, jshort};

use crate::execution::operators::ExecutionError;
use datafusion_comet_spark_expr::SparkError;
use jni::objects::{GlobalRef, JThrowable, JValue};
use jni::JNIEnv;
use lazy_static::lazy_static;
use parquet::errors::ParquetError;
use thiserror::Error;

lazy_static! {
    static ref PANIC_BACKTRACE: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
}

#[derive(thiserror::Error, Debug)]
pub enum CometError {
    #[error("Configuration Error: {0}")]
    Config(String),

    #[error("{0}")]
    NullPointer(String),

    #[error("Out of bounds{0}")]
    IndexOutOfBounds(usize),

    #[error("Comet Internal Error: {0}")]
    Internal(String),

    /// CometError::Spark is typically used in native code to emulate the same errors
    /// that Spark would return
    #[error(transparent)]
    Spark(SparkError),

    #[error(transparent)]
    Arrow {
        #[from]
        source: ArrowError,
    },

    #[error(transparent)]
    Parquet {
        #[from]
        source: ParquetError,
    },

    #[error(transparent)]
    Expression {
        #[from]
        source: ExpressionError,
    },

    #[error(transparent)]
    Execution {
        #[from]
        source: ExecutionError,
    },

    #[error(transparent)]
    IO {
        #[from]
        source: std::io::Error,
    },

    #[error(transparent)]
    NumberIntFormat {
        #[from]
        source: std::num::ParseIntError,
    },

    #[error(transparent)]
    NumberFloatFormat {
        #[from]
        source: std::num::ParseFloatError,
    },
    #[error(transparent)]
    BoolFormat {
        #[from]
        source: std::str::ParseBoolError,
    },
    #[error(transparent)]
    Format {
        #[from]
        source: Utf8Error,
    },

    #[error(transparent)]
    JNI {
        #[from]
        source: jni::errors::Error,
    },

    #[error("{msg}")]
    Panic { msg: String },

    #[error("{msg}")]
    DataFusion {
        msg: String,
        #[source]
        source: DataFusionError,
    },

    #[error("{class}: {msg}")]
    JavaException {
        class: String,
        msg: String,
        throwable: GlobalRef,
    },
}

pub fn init() {
    std::panic::set_hook(Box::new(|_panic_info| {
        // Capture the backtrace for a panic
        *PANIC_BACKTRACE.lock().unwrap() =
            Some(std::backtrace::Backtrace::force_capture().to_string());
    }));
}

/// Converts the results from `panic::catch_unwind` (e.g. a panic) to a `CometError`
impl convert::From<Box<dyn Any + Send>> for CometError {
    fn from(e: Box<dyn Any + Send>) -> Self {
        CometError::Panic {
            msg: match e.downcast_ref::<&str>() {
                Some(s) => s.to_string(),
                None => match e.downcast_ref::<String>() {
                    Some(msg) => msg.to_string(),
                    None => "unknown panic".to_string(),
                },
            },
        }
    }
}

impl From<DataFusionError> for CometError {
    fn from(value: DataFusionError) -> Self {
        CometError::DataFusion {
            msg: value.message().to_string(),
            source: value,
        }
    }
}

impl From<CometError> for DataFusionError {
    fn from(value: CometError) -> Self {
        match value {
            CometError::DataFusion { msg: _, source } => source,
            _ => DataFusionError::Execution(value.to_string()),
        }
    }
}

impl From<CometError> for ExecutionError {
    fn from(value: CometError) -> Self {
        match value {
            CometError::Execution { source } => source,
            CometError::JavaException {
                class,
                msg,
                throwable,
            } => ExecutionError::JavaException {
                class,
                msg,
                throwable,
            },
            _ => ExecutionError::GeneralError(value.to_string()),
        }
    }
}

impl jni::errors::ToException for CometError {
    fn to_exception(&self) -> Exception {
        match self {
            CometError::IndexOutOfBounds(..) => Exception {
                class: "java/lang/IndexOutOfBoundsException".to_string(),
                msg: self.to_string(),
            },
            CometError::NullPointer(..) => Exception {
                class: "java/lang/NullPointerException".to_string(),
                msg: self.to_string(),
            },
            CometError::Spark { .. } => Exception {
                class: "org/apache/spark/SparkException".to_string(),
                msg: self.to_string(),
            },
            CometError::NumberIntFormat { source: s } => Exception {
                class: "java/lang/NumberFormatException".to_string(),
                msg: s.to_string(),
            },
            CometError::NumberFloatFormat { source: s } => Exception {
                class: "java/lang/NumberFormatException".to_string(),
                msg: s.to_string(),
            },
            CometError::IO { .. } => Exception {
                class: "java/io/IOException".to_string(),
                msg: self.to_string(),
            },
            CometError::Parquet { .. } => Exception {
                class: "org/apache/comet/ParquetRuntimeException".to_string(),
                msg: self.to_string(),
            },
            _other => Exception {
                class: "org/apache/comet/CometNativeException".to_string(),
                msg: self.to_string(),
            },
        }
    }
}

/// Error returned when there is an error during executing an expression.
#[derive(thiserror::Error, Debug)]
pub enum ExpressionError {
    /// Simple error
    #[error("General expression error with reason {0}.")]
    General(String),

    /// Deserialization error
    #[error("Fail to deserialize to native expression with reason {0}.")]
    Deserialize(String),

    /// Evaluation error
    #[error("Fail to evaluate native expression with reason {0}.")]
    Evaluation(String),

    /// Error when processing Arrow array.
    #[error("Fail to process Arrow array with reason {0}.")]
    ArrowError(String),
}

/// A specialized `Result` for Comet errors.
pub type CometResult<T> = result::Result<T, CometError>;

// ----------------------------------------------------------------------
// Convenient macros for different errors

macro_rules! general_err {
    ($fmt:expr, $($args:expr),*) => (crate::CometError::from(parquet::errors::ParquetError::General(format!($fmt, $($args),*))));
}

/// Returns the "default value" for a type.  This is used for JNI code in order to facilitate
/// returning a value in cases where an exception is thrown.  This value will never be used, as the
/// JVM will note the pending exception.
///
/// Default values are often some kind of initial value, identity value, or anything else that
/// may make sense as a default.
///
/// NOTE: We can't just use [Default] since both the trait and the object are defined in other
/// crates.
/// See [Rust Compiler Error Index - E0117](https://doc.rust-lang.org/error-index.html#E0117)
pub trait JNIDefault {
    fn default() -> Self;
}

impl JNIDefault for jboolean {
    fn default() -> jboolean {
        0
    }
}

impl JNIDefault for jbyte {
    fn default() -> jbyte {
        0
    }
}

impl JNIDefault for jchar {
    fn default() -> jchar {
        0
    }
}

impl JNIDefault for jdouble {
    fn default() -> jdouble {
        0.0
    }
}

impl JNIDefault for jfloat {
    fn default() -> jfloat {
        0.0
    }
}

impl JNIDefault for jint {
    fn default() -> jint {
        0
    }
}

impl JNIDefault for jlong {
    fn default() -> jlong {
        0
    }
}

/// The "default value" for all returned objects, such as [jstring], [jlongArray], etc.
impl JNIDefault for jobject {
    fn default() -> jobject {
        std::ptr::null_mut()
    }
}

impl JNIDefault for jshort {
    fn default() -> jshort {
        0
    }
}

impl JNIDefault for () {
    fn default() {}
}

// Unwrap the result returned from `panic::catch_unwind` when `Ok`, otherwise throw a
// `RuntimeException` back to the calling Java.  Since a return result is required, use `JNIDefault`
// to create a reasonable result.  This returned default value will be ignored due to the exception.
pub fn unwrap_or_throw_default<T: JNIDefault>(
    env: &mut JNIEnv,
    result: std::result::Result<T, CometError>,
) -> T {
    match result {
        Ok(value) => value,
        Err(err) => {
            let backtrace = match err {
                CometError::Panic { msg: _ } => PANIC_BACKTRACE.lock().unwrap().take(),
                _ => None,
            };
            throw_exception(env, &err, backtrace);
            T::default()
        }
    }
}

fn throw_exception(env: &mut JNIEnv, error: &CometError, backtrace: Option<String>) {
    // If there isn't already an exception?
    if env.exception_check().is_ok() {
        // ... then throw new exception
        match error {
            CometError::JavaException {
                class: _,
                msg: _,
                throwable,
            } => env.throw(<&JThrowable>::from(throwable.as_obj())),
            CometError::Execution {
                source:
                    ExecutionError::JavaException {
                        class: _,
                        msg: _,
                        throwable,
                    },
            } => env.throw(<&JThrowable>::from(throwable.as_obj())),
            CometError::DataFusion {
                msg: _,
                source: DataFusionError::External(e),
            } if matches!(e.downcast_ref(), Some(SparkError::CastOverFlow { .. })) => {
                match e.downcast_ref() {
                    Some(SparkError::CastOverFlow {
                        value,
                        from_type,
                        to_type,
                    }) => {
                        let throwable: JThrowable = env
                            .new_object(
                                "org/apache/spark/sql/comet/CastOverflowException",
                                "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
                                &[
                                    JValue::Object(&env.new_string(value).unwrap()),
                                    JValue::Object(&env.new_string(from_type).unwrap()),
                                    JValue::Object(&env.new_string(to_type).unwrap()),
                                ],
                            )
                            .unwrap()
                            .into();
                        env.throw(throwable)
                    }
                    _ => unreachable!(),
                }
            }
            _ => {
                let exception = error.to_exception();
                match backtrace {
                    Some(backtrace_string) => env.throw_new(
                        exception.class,
                        to_stacktrace_string(exception.msg, backtrace_string).unwrap(),
                    ),
                    _ => env.throw_new(exception.class, exception.msg),
                }
            }
        }
        .expect("Thrown exception")
    }
}

#[derive(Debug, Error)]
enum StacktraceError {
    #[error("Unable to initialize message: {0}")]
    Message(String),
    #[error("Unable to initialize backtrace regex: {0}")]
    Regex(#[from] regex::Error),
    #[error("Required field missing: {0}")]
    Required_Field(String),
    #[error("Unable to format stacktrace element: {0}")]
    Element(#[from] std::fmt::Error),
}

fn to_stacktrace_string(msg: String, backtrace_string: String) -> Result<String, StacktraceError> {
    let mut res = String::new();
    write!(&mut res, "{}", msg).map_err(|error| StacktraceError::Message(error.to_string()))?;

    // Use multi-line mode and named capture groups to identify the following stacktrace fields:
    // - dc = declaredClass
    // - mn = methodName
    // - fn = fileName (optional)
    // - line = file line number (optional)
    // - col = file col number within the line (optional)
    let re = Regex::new(
        r"(?m)^\s*\d+: (?<dc>.*?)(?<mn>[^:]+)\n(\s*at\s+(?<fn>[^:]+):(?<line>\d+):(?<col>\d+)$)?",
    )?;
    for c in re.captures_iter(backtrace_string.as_str()) {
        write!(
            &mut res,
            "\n        at {}{}({}:{})",
            c.name("dc")
                .ok_or_else(|| StacktraceError::Required_Field("declared class".to_string()))?
                .as_str(),
            c.name("mn")
                .ok_or_else(|| StacktraceError::Required_Field("method name".to_string()))?
                .as_str(),
            // There are internal calls within the backtrace that don't provide file information
            c.name("fn").map(|m| m.as_str()).unwrap_or("__internal__"),
            c.name("line")
                .map(|m| m.as_str().parse().expect("numeric line number"))
                .unwrap_or(0)
        )?;
    }

    Ok(res)
}

fn flatten<T, E>(result: Result<Result<T, E>, E>) -> Result<T, E> {
    result.and_then(convert::identity)
}

// Implements "currying" from `FnOnce(T) -> R` to `FnOnce() -> R`, given
// an instance of T. Curring is not supported in Rust so we have to use this
// custom function to achieve something similar here.
fn curry<'a, T: 'a, F, R>(f: F, t: T) -> impl FnOnce() -> R + 'a
where
    F: FnOnce(T) -> R + 'a,
{
    || f(t)
}

// It is currently undefined behavior to unwind from Rust code into foreign code, so we can wrap
// our JNI functions and turn these panics into a `RuntimeException`.
pub fn try_unwrap_or_throw<T, F>(env: &JNIEnv, f: F) -> T
where
    T: JNIDefault,
    F: FnOnce(JNIEnv) -> Result<T, CometError> + UnwindSafe,
{
    let mut env1 = unsafe { JNIEnv::from_raw(env.get_raw()).unwrap() };
    let env2 = unsafe { JNIEnv::from_raw(env.get_raw()).unwrap() };
    unwrap_or_throw_default(
        &mut env1,
        flatten(catch_unwind(curry(f, env2)).map_err(CometError::from)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::File,
        io,
        io::Read,
        path::PathBuf,
        sync::{Arc, Once},
    };

    use jni::{
        objects::{JClass, JIntArray, JString, JThrowable},
        sys::{jintArray, jstring},
        AttachGuard, InitArgsBuilder, JNIEnv, JNIVersion, JavaVM,
    };

    use assertables::{assert_starts_with, assert_starts_with_as_result};

    pub fn jvm() -> &'static Arc<JavaVM> {
        static mut JVM: Option<Arc<JavaVM>> = None;
        static INIT: Once = Once::new();

        // Capture panic backtraces
        init();

        INIT.call_once(|| {
            // Add common classes to the classpath in so that we can find CometException
            let mut common_classes = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            common_classes.push("../../common/target/classes");
            let mut class_path = common_classes
                .as_path()
                .to_str()
                .expect("common classes as an str")
                .to_string();
            class_path.insert_str(0, "-Djava.class.path=");

            // Build the VM properties
            let jvm_args = InitArgsBuilder::new()
                // Pass the JNI API version (default is 8)
                .version(JNIVersion::V8)
                // You can additionally pass any JVM options (standard, like a system property,
                // or VM-specific).
                // Here we enable some extra JNI checks useful during development
                .option("-Xcheck:jni")
                .option(class_path.as_str())
                .build()
                .unwrap_or_else(|e| panic!("{:#?}", e));

            let jvm = JavaVM::new(jvm_args).unwrap_or_else(|e| panic!("{:#?}", e));

            #[allow(static_mut_refs)]
            unsafe {
                JVM = Some(Arc::new(jvm));
            }
        });

        #[allow(static_mut_refs)]
        unsafe {
            JVM.as_ref().unwrap()
        }
    }

    fn attach_current_thread() -> AttachGuard<'static> {
        jvm().attach_current_thread().expect("Unable to attach JVM")
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn error_from_panic() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        try_unwrap_or_throw(&env, |_| -> CometResult<()> {
            panic!("oops!");
        });

        assert_pending_java_exception_detailed(
            &mut env,
            Some("java/lang/RuntimeException"),
            Some("oops!"),
        );
    }

    // Verify that functions that return an object are handled correctly.  This is basically
    // a test of the "happy path".
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn object_result() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        let clazz = env.find_class("java/lang/Object").unwrap();
        let input = env.new_string("World".to_string()).unwrap();

        let actual = Java_Errors_hello(&env, clazz, input);
        let actual_s = unsafe { JString::from_raw(actual) };

        let actual_string = String::from(env.get_string(&actual_s).unwrap().to_str().unwrap());
        assert_eq!("Hello, World!", actual_string);
    }

    // Verify that functions that return an native time are handled correctly.  This is basically
    // a test of the "happy path".
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn jlong_result() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        // Class java.lang.object is just a stand-in
        let class = env.find_class("java/lang/Object").unwrap();
        let a: jlong = 6;
        let b: jlong = 3;
        let actual = Java_Errors_div(&env, class, a, b);

        assert_eq!(2, actual);
    }

    // Verify that functions that return an array can handle throwing exceptions.  The test
    // causes an exception by dividing by zero.
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn jlong_panic_exception() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        // Class java.lang.object is just a stand-in
        let class = env.find_class("java/lang/Object").unwrap();
        let a: jlong = 6;
        let b: jlong = 0;
        let _actual = Java_Errors_div(&env, class, a, b);

        assert_pending_java_exception_detailed(
            &mut env,
            Some("java/lang/RuntimeException"),
            Some("attempt to divide by zero"),
        );
    }

    // Verify that functions that return an native time are handled correctly.  This is basically
    // a test of the "happy path".
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn jlong_result_ok() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        // Class java.lang.object is just a stand-in
        let class = env.find_class("java/lang/Object").unwrap();
        let a: JString = env.new_string("9".to_string()).unwrap();
        let b: JString = env.new_string("3".to_string()).unwrap();
        let actual = Java_Errors_div_with_parse(&env, class, a, b);

        assert_eq!(3, actual);
    }

    // Verify that functions that return an native time are handled correctly.  This is basically
    // a test of the "happy path".
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn jlong_result_err() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        // Class java.lang.object is just a stand-in
        let class = env.find_class("java/lang/Object").unwrap();
        let a: JString = env.new_string("NaN".to_string()).unwrap();
        let b: JString = env.new_string("3".to_string()).unwrap();
        let _actual = Java_Errors_div_with_parse(&env, class, a, b);

        assert_pending_java_exception_detailed(
            &mut env,
            Some("java/lang/NumberFormatException"),
            Some("invalid digit found in string"),
        );
    }

    // Verify that functions that return an array are handled correctly.  This is basically
    // a test of the "happy path".
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn jint_array_result() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        // Class java.lang.object is just a stand-in
        let class = env.find_class("java/lang/Object").unwrap();
        let buf = [2, 4, 6];
        let input = env.new_int_array(3).unwrap();
        env.set_int_array_region(&input, 0, &buf).unwrap();
        let actual = Java_Errors_array_div(&env, class, &input, 2);
        let actual_s = unsafe { JIntArray::from_raw(actual) };

        let mut buf: [i32; 3] = [0; 3];
        env.get_int_array_region(&actual_s, 0, &mut buf).unwrap();
        assert_eq!([1, 2, 3], buf);
    }

    // Verify that functions that return an array can handle throwing exceptions.  The test
    // causes an exception by dividing by zero.
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn jint_array_panic_exception() {
        let _guard = attach_current_thread();
        let mut env = jvm().get_env().unwrap();

        // Class java.lang.object is just a stand-in
        let class = env.find_class("java/lang/Object").unwrap();
        let buf = [2, 4, 6];
        let input = env.new_int_array(3).unwrap();
        env.set_int_array_region(&input, 0, &buf).unwrap();
        let _actual = Java_Errors_array_div(&env, class, &input, 0);

        assert_pending_java_exception_detailed(
            &mut env,
            Some("java/lang/RuntimeException"),
            Some("attempt to divide by zero"),
        );
    }

    /// Test that conversion of a serialized backtrace to an equivalent stacktrace message.
    ///
    /// See [`object_panic_exception`] for a test which involves generating a panic and verifying
    /// that the resulting stack trace includes the offending call.
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `dlopen`
    pub fn stacktrace_string() {
        // Setup: Start with a backtrace that includes all of the expected scenarios, including
        // cases where the file and location are not provided as part of the backtrace capture
        let backtrace_string = read_resource("testdata/backtrace.txt").expect("backtrace content");

        // Test: Reformat the serialized backtrace as a multi-line message which includes the
        // backtrace formatted as a stacktrace
        let stacktrace_string =
            to_stacktrace_string("Some Error Message".to_string(), backtrace_string).unwrap();

        // Verify: The message matches the expected output.  Trim the expected string to remove
        // the carriage return
        let expected_string = read_resource("testdata/stacktrace.txt").expect("stacktrace content");
        assert_eq!(expected_string.trim(), stacktrace_string.as_str());
    }

    fn read_resource(path: &str) -> Result<String, io::Error> {
        let mut path_buf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path_buf.push(path);

        let mut f = File::open(path_buf.as_path())?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        Ok(s)
    }

    // Example of a simple JNI "Hello World" program.  It can be used to demonstrate:
    // * returning an object
    // * throwing an exception from `.expect()`
    #[no_mangle]
    pub extern "system" fn Java_Errors_hello(
        e: &JNIEnv,
        _class: JClass,
        input: JString,
    ) -> jstring {
        try_unwrap_or_throw(e, |mut env| {
            let input: String = env
                .get_string(&input)
                .expect("Couldn't get java string!")
                .into();

            let output = env
                .new_string(format!("Hello, {}!", input))
                .expect("Couldn't create java string!");

            Ok(output.into_raw())
        })
    }

    // Example of a simple JNI function that divides.  It can be used to demonstrate:
    // * returning an native type
    // * throwing an exception when dividing by zero
    #[no_mangle]
    pub extern "system" fn Java_Errors_div(
        env: &JNIEnv,
        _class: JClass,
        a: jlong,
        b: jlong,
    ) -> jlong {
        try_unwrap_or_throw(env, |_| Ok(a / b))
    }

    #[no_mangle]
    pub extern "system" fn Java_Errors_div_with_parse(
        e: &JNIEnv,
        _class: JClass,
        a: JString,
        b: JString,
    ) -> jlong {
        try_unwrap_or_throw(e, |mut env| {
            let a_value: i64 = env.get_string(&a)?.to_str()?.parse()?;
            let b_value: i64 = env.get_string(&b)?.to_str()?.parse()?;
            Ok(a_value / b_value)
        })
    }

    // Example of a simple JNI function that divides.  It can be used to demonstrate:
    // * returning an array
    // * throwing an exception when dividing by zero
    #[no_mangle]
    pub extern "system" fn Java_Errors_array_div(
        e: &JNIEnv,
        _class: JClass,
        input: &JIntArray,
        divisor: jint,
    ) -> jintArray {
        try_unwrap_or_throw(e, |env| {
            let mut input_buf: [jint; 3] = [0; 3];
            env.get_int_array_region(input, 0, &mut input_buf)?;

            let buf = input_buf.map(|v| -> jint { v / divisor });

            let result = env.new_int_array(3)?;
            env.set_int_array_region(&result, 0, &buf)?;
            Ok(result.into_raw())
        })
    }

    // Helper method that asserts there is a pending Java exception which is an `instance_of`
    // `expected_type` with a message matching `expected_message` and clears it if any.
    fn assert_pending_java_exception_detailed(
        env: &mut JNIEnv,
        expected_type: Option<&str>,
        expected_message: Option<&str>,
    ) {
        assert!(env.exception_check().unwrap());
        let exception = env.exception_occurred().expect("Unable to get exception");
        env.exception_clear().unwrap();

        if let Some(expected_type) = expected_type {
            assert_exception_type(env, &exception, expected_type);
        }

        if let Some(expected_message) = expected_message {
            assert_exception_message(env, exception, expected_message);
        }
    }

    // Asserts that exception is an `instance_of` `expected_type` type.
    fn assert_exception_type(env: &mut JNIEnv, exception: &JThrowable, expected_type: &str) {
        if !env.is_instance_of(exception, expected_type).unwrap() {
            let class: JClass = env.get_object_class(exception).unwrap();
            let name = env
                .call_method(class, "getName", "()Ljava/lang/String;", &[])
                .unwrap()
                .l()
                .unwrap();
            let name_string = name.into();
            let class_name: String = env.get_string(&name_string).unwrap().into();
            assert_eq!(class_name.replace('.', "/"), expected_type);
        };
    }

    // Asserts that exception's message matches `expected_message`.
    fn assert_exception_message(env: &mut JNIEnv, exception: JThrowable, expected_message: &str) {
        let message = env
            .call_method(exception, "getMessage", "()Ljava/lang/String;", &[])
            .unwrap()
            .l()
            .unwrap();
        let message_string = message.into();
        let msg_rust: String = env.get_string(&message_string).unwrap().into();
        println!("{}", msg_rust);
        // Since panics result in multi-line messages which include the backtrace, just use the
        // first line.
        assert_starts_with!(msg_rust, expected_message);
    }
}
