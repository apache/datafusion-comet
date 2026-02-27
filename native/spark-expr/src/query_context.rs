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

//! Query execution context for error reporting
//!
//! This module provides QueryContext which mirrors Spark's SQLQueryContext
//! for providing SQL text, line/position information, and error location
//! pointers in exception messages.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Based on Spark's SQLQueryContext for error reporting.
///
/// Contains information about where an error occurred in a SQL query,
/// including the full SQL text, line/column positions, and object context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryContext {
    /// Full SQL query text
    #[serde(rename = "sqlText")]
    pub sql_text: Arc<String>,

    /// Start offset in SQL text (0-based, character index)
    #[serde(rename = "startIndex")]
    pub start_index: i32,

    /// Stop offset in SQL text (0-based, character index, inclusive)
    #[serde(rename = "stopIndex")]
    pub stop_index: i32,

    /// Object type (e.g., "VIEW", "Project", "Filter")
    #[serde(rename = "objectType", skip_serializing_if = "Option::is_none")]
    pub object_type: Option<String>,

    /// Object name (e.g., view name, column name)
    #[serde(rename = "objectName", skip_serializing_if = "Option::is_none")]
    pub object_name: Option<String>,

    /// Line number in SQL query (1-based)
    pub line: i32,

    /// Column position within the line (0-based)
    #[serde(rename = "startPosition")]
    pub start_position: i32,
}

impl QueryContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sql_text: String,
        start_index: i32,
        stop_index: i32,
        object_type: Option<String>,
        object_name: Option<String>,
        line: i32,
        start_position: i32,
    ) -> Self {
        Self {
            sql_text: Arc::new(sql_text),
            start_index,
            stop_index,
            object_type,
            object_name,
            line,
            start_position,
        }
    }

    /// Convert a character index to a byte offset in the SQL text.
    /// Returns None if the character index is out of range.
    fn char_index_to_byte_offset(&self, char_index: usize) -> Option<usize> {
        self.sql_text
            .char_indices()
            .nth(char_index)
            .map(|(byte_offset, _)| byte_offset)
    }

    /// Generate a summary string showing SQL fragment with error location.
    /// (From SQLQueryContext.summary)
    ///
    /// Format example:
    /// ```text
    /// == SQL of VIEW v1 (line 1, position 8) ==
    /// SELECT a/b FROM t
    ///        ^^^
    /// ```
    pub fn format_summary(&self) -> String {
        let start_char = self.start_index.max(0) as usize;
        // stop_index is inclusive; fragment covers [start, stop]
        let stop_char = (self.stop_index + 1).max(0) as usize;

        let fragment = match (
            self.char_index_to_byte_offset(start_char),
            // stop_char may equal sql_text.chars().count() (one past the end)
            self.char_index_to_byte_offset(stop_char).or_else(|| {
                if stop_char == self.sql_text.chars().count() {
                    Some(self.sql_text.len())
                } else {
                    None
                }
            }),
        ) {
            (Some(start_byte), Some(stop_byte)) => &self.sql_text[start_byte..stop_byte],
            _ => "",
        };

        // Build the header line
        let mut summary = String::from("== SQL");

        if let Some(obj_type) = &self.object_type {
            if !obj_type.is_empty() {
                summary.push_str(" of ");
                summary.push_str(obj_type);

                if let Some(obj_name) = &self.object_name {
                    if !obj_name.is_empty() {
                        summary.push(' ');
                        summary.push_str(obj_name);
                    }
                }
            }
        }

        summary.push_str(&format!(
            " (line {}, position {}) ==\n",
            self.line,
            self.start_position + 1 // Convert 0-based to 1-based for display
        ));

        // Add the SQL text with fragment highlighted
        summary.push_str(&self.sql_text);
        summary.push('\n');

        // Add caret pointer
        let caret_position = self.start_position.max(0) as usize;
        summary.push_str(&" ".repeat(caret_position));
        // fragment.chars().count() gives the correct display width for non-ASCII
        summary.push_str(&"^".repeat(fragment.chars().count().max(1)));

        summary
    }

    /// Returns the SQL fragment that caused the error.
    pub fn fragment(&self) -> String {
        let start_char = self.start_index.max(0) as usize;
        let stop_char = (self.stop_index + 1).max(0) as usize;

        match (
            self.char_index_to_byte_offset(start_char),
            self.char_index_to_byte_offset(stop_char).or_else(|| {
                if stop_char == self.sql_text.chars().count() {
                    Some(self.sql_text.len())
                } else {
                    None
                }
            }),
        ) {
            (Some(start_byte), Some(stop_byte)) => self.sql_text[start_byte..stop_byte].to_string(),
            _ => String::new(),
        }
    }
}

use std::collections::HashMap;
use std::sync::RwLock;

/// Map that stores QueryContext information for expressions during execution.
///
/// This map is populated during plan deserialization and accessed
/// during error creation to attach SQL context to exceptions.
#[derive(Debug)]
pub struct QueryContextMap {
    /// Map from expression ID to QueryContext
    contexts: RwLock<HashMap<u64, Arc<QueryContext>>>,
}

impl QueryContextMap {
    pub fn new() -> Self {
        Self {
            contexts: RwLock::new(HashMap::new()),
        }
    }

    /// Register a QueryContext for an expression ID.
    ///
    /// If the expression ID already exists, it will be replaced.
    ///
    /// # Arguments
    /// * `expr_id` - Unique expression identifier from protobuf
    /// * `context` - QueryContext containing SQL text and position info
    pub fn register(&self, expr_id: u64, context: QueryContext) {
        let mut contexts = self.contexts.write().unwrap();
        contexts.insert(expr_id, Arc::new(context));
    }

    /// Get the QueryContext for an expression ID.
    ///
    /// Returns None if no context is registered for this expression.
    ///
    /// # Arguments
    /// * `expr_id` - Expression identifier to look up
    pub fn get(&self, expr_id: u64) -> Option<Arc<QueryContext>> {
        let contexts = self.contexts.read().unwrap();
        contexts.get(&expr_id).cloned()
    }

    /// Clear all registered contexts.
    ///
    /// This is typically called after plan execution completes to free memory.
    pub fn clear(&self) {
        let mut contexts = self.contexts.write().unwrap();
        contexts.clear();
    }

    /// Return the number of registered contexts (for debugging/testing)
    pub fn len(&self) -> usize {
        let contexts = self.contexts.read().unwrap();
        contexts.len()
    }

    /// Check if the map is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for QueryContextMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a new session-scoped QueryContextMap.
///
/// This should be called once per SessionContext during plan creation
/// and passed to expressions that need query context for error reporting.
pub fn create_query_context_map() -> Arc<QueryContextMap> {
    Arc::new(QueryContextMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_context_creation() {
        let ctx = QueryContext::new(
            "SELECT a/b FROM t".to_string(),
            7,
            9,
            Some("Divide".to_string()),
            Some("a/b".to_string()),
            1,
            7,
        );

        assert_eq!(*ctx.sql_text, "SELECT a/b FROM t");
        assert_eq!(ctx.start_index, 7);
        assert_eq!(ctx.stop_index, 9);
        assert_eq!(ctx.object_type, Some("Divide".to_string()));
        assert_eq!(ctx.object_name, Some("a/b".to_string()));
        assert_eq!(ctx.line, 1);
        assert_eq!(ctx.start_position, 7);
    }

    #[test]
    fn test_query_context_serialization() {
        let ctx = QueryContext::new(
            "SELECT a/b FROM t".to_string(),
            7,
            9,
            Some("Divide".to_string()),
            Some("a/b".to_string()),
            1,
            7,
        );

        let json = serde_json::to_string(&ctx).unwrap();
        let deserialized: QueryContext = serde_json::from_str(&json).unwrap();

        assert_eq!(ctx, deserialized);
    }

    #[test]
    fn test_format_summary() {
        let ctx = QueryContext::new(
            "SELECT a/b FROM t".to_string(),
            7,
            9,
            Some("VIEW".to_string()),
            Some("v1".to_string()),
            1,
            7,
        );

        let summary = ctx.format_summary();

        assert!(summary.contains("== SQL of VIEW v1 (line 1, position 8) =="));
        assert!(summary.contains("SELECT a/b FROM t"));
        assert!(summary.contains("^^^")); // Three carets for "a/b"
    }

    #[test]
    fn test_format_summary_without_object() {
        let ctx = QueryContext::new("SELECT a/b FROM t".to_string(), 7, 9, None, None, 1, 7);

        let summary = ctx.format_summary();

        assert!(summary.contains("== SQL (line 1, position 8) =="));
        assert!(summary.contains("SELECT a/b FROM t"));
    }

    #[test]
    fn test_fragment() {
        let ctx = QueryContext::new("SELECT a/b FROM t".to_string(), 7, 9, None, None, 1, 7);

        assert_eq!(ctx.fragment(), "a/b");
    }

    #[test]
    fn test_arc_string_sharing() {
        let ctx1 = QueryContext::new("SELECT a/b FROM t".to_string(), 7, 9, None, None, 1, 7);

        let ctx2 = ctx1.clone();

        // Arc should share the same allocation
        assert!(Arc::ptr_eq(&ctx1.sql_text, &ctx2.sql_text));
    }

    #[test]
    fn test_json_with_optional_fields() {
        let ctx = QueryContext::new("SELECT a/b FROM t".to_string(), 7, 9, None, None, 1, 7);

        let json = serde_json::to_string(&ctx).unwrap();

        // Should not serialize objectType and objectName when None
        assert!(!json.contains("objectType"));
        assert!(!json.contains("objectName"));
    }

    #[test]
    fn test_map_register_and_get() {
        let map = QueryContextMap::new();

        let ctx = QueryContext::new("SELECT a/b FROM t".to_string(), 7, 9, None, None, 1, 7);

        map.register(1, ctx.clone());

        let retrieved = map.get(1).unwrap();
        assert_eq!(*retrieved.sql_text, "SELECT a/b FROM t");
        assert_eq!(retrieved.start_index, 7);
    }

    #[test]
    fn test_map_get_nonexistent() {
        let map = QueryContextMap::new();
        assert!(map.get(999).is_none());
    }

    #[test]
    fn test_map_clear() {
        let map = QueryContextMap::new();

        let ctx = QueryContext::new("SELECT a/b FROM t".to_string(), 7, 9, None, None, 1, 7);

        map.register(1, ctx);
        assert_eq!(map.len(), 1);

        map.clear();
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
    }

    // Verify that fragment() and format_summary() correctly handle SQL text that
    // contains multi-byte characters

    #[test]
    fn test_fragment_non_ascii_accented() {
        // "é" is a 2-byte UTF-8 sequence (U+00E9).
        // SQL: "SELECT café FROM t"
        //       0123456789...
        // char indices: c=7, a=8, f=9, é=10, ' '=11 ...  FROM = 12..
        // start_index=7, stop_index=10 should yield "café"
        let sql = "SELECT café FROM t".to_string();
        let ctx = QueryContext::new(sql, 7, 10, None, None, 1, 7);
        assert_eq!(ctx.fragment(), "café");
    }
}
