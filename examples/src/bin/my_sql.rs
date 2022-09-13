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

use ballista::prelude::*;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    prelude::CsvReadOptions,
};

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results, using SQL
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;
    let ctx = BallistaContext::remote("localhost", 50050, &config).await?;

    let schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int32, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int32, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]);

    // register csv file with the execution context
    let path = "../ballista/rust/scheduler/testdata/lineitem";
    let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .has_header(false)
            .file_extension(".tbl");
    ctx.register_csv("lineitem", path, options).await?;

    let df = ctx
        .sql(
            "select l_returnflag, sum(l_extendedprice * 1) as sum_disc_price
        from lineitem
        group by l_returnflag
        order by l_returnflag",
        )
        .await?;

    let _df = ctx
        .sql(
            "select l_returnflag, l_extendedprice from lineitem",
        )
        .await?;

    let plan = df.to_logical_plan()?;
    println!("logical_plan: {:#?}", plan);

    // print the results
    df.show().await?;

    Ok(())
}
