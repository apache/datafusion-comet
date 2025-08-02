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

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Errors which can occur during accessing Hdfs cluster
#[derive(Debug)]
pub enum HdfsErr {
    Generic(String),
    /// file path
    FileNotFound(String),
    /// file path           
    FileAlreadyExists(String),
    /// name node address
    CannotConnectToNameNode(String),
    /// URL
    InvalidUrl(String),
}

impl Display for HdfsErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HdfsErr::FileNotFound(path) => write!(f, "Hdfs file {path} not found"),
            HdfsErr::FileAlreadyExists(path) => {
                write!(f, "Hdfs file {path} already exists")
            }
            HdfsErr::InvalidUrl(path) => write!(f, "Hdfs url {path} is not valid"),
            HdfsErr::CannotConnectToNameNode(namenode_uri) => {
                write!(f, "Cannot connect to name node {namenode_uri}")
            }
            HdfsErr::Generic(err_str) => write!(f, "Generic error with msg: {err_str}"),
        }
    }
}

impl Error for HdfsErr {}
