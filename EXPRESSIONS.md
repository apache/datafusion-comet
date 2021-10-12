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

# Expressions Supported by Comet

The following Spark expressions are currently available:

+ Literals
+ Arithmetic Operators
    + UnaryMinus
    + Add/Minus/Multiply/Divide/Remainder
+ Conditional functions
    + Case When
    + If
+ Cast
+ Coalesce
+ Boolean functions
    + And
    + Or
    + Not
    + EqualTo
    + EqualNullSafe
    + GreaterThan
    + GreaterThanOrEqual
    + LessThan
    + LessThanOrEqual
    + IsNull
    + IsNotNull
    + In
+ String functions
    + Substring
    + Coalesce
    + StringSpace
    + Like
    + Contains
    + Startswith
    + Endswith
    + Ascii
    + Bit_length
    + Octet_length
    + Upper
    + Lower
    + Chr
    + Initcap
    + Trim/Btrim/Ltrim/Rtrim
    + Concat_ws
    + Repeat
    + Length
    + Reverse
    + Instr
    + Replace
    + Translate
+ Bitwise functions
    + Shiftright/Shiftleft
+ Date/Time functions
    + Year/Hour/Minute/Second
+ Math functions
    + Abs
    + Acos
    + Asin
    + Atan
    + Atan2
    + Cos
    + Exp
    + Ln
    + Log10
    + Log2
    + Pow
    + Round
    + Signum
    + Sin
    + Sqrt
    + Tan
    + Ceil
    + Floor
+ Aggregate functions
    + Count
    + Sum
    + Max
    + Min
