/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
extern crate omnisci_log_scraper;
use omnisci_log_scraper::log_parser as olog;

fn pln(line: &str) -> olog::LogLine {
    let mut rec = olog::LogLine::new(line.trim()).unwrap();
    rec.parse_msg();
    rec
}

#[test]
fn test_sql_execute() {
    let rec = pln(r#"
2020-07-15T08:27:53.512388 I 16 DBHandler.cpp:964 stdlog sql_execute 1 1769 omnisci admin 751-UnTT {"query_str","client","execution_time_ms","total_time_ms"} {"SELECT count(*) from omnisci_states;","tcp:localhost:47712","1764","1768"}
"#);
    assert_eq!(rec.fileline, "DBHandler.cpp:964");
    assert_eq!(rec.username.unwrap(), "admin");
    assert_eq!(rec.event.unwrap(), "sql_execute");
    assert_eq!(rec.query.unwrap(), "SELECT count(*) from omnisci_states;");
}

#[test]
fn sql_execute_begin() {
    let rec = pln(r#"
2020-09-23T00:08:21.127508 I 1 DBHandler.cpp:1058 stdlog_begin sql_execute 1901 0 mapd admin 751-xnJg {"query_str"} {"dump table satelltelocations to '/omnisci-storage/satellites.gzip' with (compression = 'gzip');"}
"#);
    assert_eq!(rec.fileline, "DBHandler.cpp:1058");
    assert_eq!(rec.event.unwrap(), "sql_execute_begin");
    assert_eq!(rec.operation.unwrap(), "DUMP");
}

#[test]
fn sql_execute() {
    let rec = pln(r#"
2020-09-23T00:08:21.130481 I 1 DBHandler.cpp:1058 stdlog sql_execute 1901 3 mapd admin 751-xnJg {"query_str","client","nonce"} {"dump table satelltelocations to '/omnisci-storage/satellites.gzip' with (compression = 'gzip');","tcp:172.27.0.1:34034",""}
"#);
    assert_eq!(rec.fileline, "DBHandler.cpp:1058");
    assert_eq!(rec.event.unwrap(), "sql_execute");
    assert_eq!(rec.operation.unwrap(), "DUMP");
}

#[test]
fn sql_execute_nonce() {
    let rec = pln(r#"
2020-07-01T00:00:00.000106 I 15 DBHandler.cpp:1058 stdlog sql_execute 1 1425 omnisci admin 455-NzOR {"query_str","client","nonce","execution_time_ms","total_time_ms"} {"SELECT count(*) from omnisci_states;","tcp:172.17.0.1:52290","{""chartId"":""work_to_generate_logs"",""dashboardId"":100}","1418","1425"}
"#);
    assert_eq!(rec.event.unwrap(), "sql_execute");
    assert_eq!(rec.operation.unwrap(), "SELECT");
    assert_eq!(rec.query.unwrap(), "SELECT count(*) from omnisci_states;");
    assert_eq!(rec.client.unwrap(), "tcp:172.17.0.1:52290");
    assert_eq!(rec.dashboardid.unwrap(), "100");
    assert_eq!(rec.chartid.unwrap(), "work_to_generate_logs");
}

#[test]
fn render_vega_begin() {
    let rec = pln(r#"
2020-09-23T17:49:07.639975 I 1 DBHandler.cpp:3358 stdlog_begin render_vega 36 0 mapd tony 817-guRB {"widget_id","compression_level","vega_json","nonce"} {"6147859833690711","3","{""width"":679,""height"":342,""viewRenderOptions"":{""premultipliedAlpha"":false},""data"":[{""name"":""backendChoropleth"",""format"":""polys"",""sql"":""SELECT ty_flights_mpoly_table.dest_state_mpoly AS dest_state_mpoly FROM ty_flights_mpoly_table WHERE (ST_XMax(ty_flights_mpoly_table.dest_state_mpoly) >= -125.65257161471237 AND ST_XMin(ty_flights_mpoly_table.dest_state_mpoly) <= -65.67742838528845 AND ST_YMax(ty_flights_mpoly_table.dest_state_mpoly) >= 25.820000000001016 AND ST_YMin(ty_flights_mpoly_table.dest_state_mpoly) <= 49.38000000000076 AND ((((origin_state_name = 'Tennessee')))) AND ST_XMax(origin_state_mpoly) >= -125.65257161471305\n          AND ST_XMin(origin_state_mpoly) <= -65.6774283852891\n          AND ST_YMax(origin_state_mpoly) >= 25.82000000000167\n          AND ST_YMin(origin_state_mpoly) <= 49.38000000000119) AND SAMPLE_RATIO(0.0027497656099794055)"",""enableHitTesting"":true}],""scales"":[{""name"":""x"",""type"":""linear"",""domain"":[-13987580.287095958,-7311177.883458873],""range"":""width""},{""name"":""y"",""type"":""linear"",""domain"":[2976804.1982603935,6339587.146785449],""range"":""height""}],""projections"":[{""name"":""mercator_map_projection"",""type"":""mercator"",""bounds"":{""x"":[-125.65257161471237,-65.67742838528845],""y"":[25.820000000001016,49.38000000000076]}}],""marks"":[{""type"":""polys"",""from"":{""data"":""backendChoropleth""},""properties"":{""x"":{""field"":""x""},""y"":{""field"":""y""},""fillColor"":{""value"":""rgba(234,85,69,0.85)""},""strokeColor"":""white"",""strokeWidth"":1,""lineJoin"":""miter"",""miterLimit"":10},""transform"":{""projection"":""mercator_map_projection""}}]}","15"}
"#);
    assert_eq!(rec.event.unwrap(), "render_vega_begin");
    assert_eq!(rec.query.unwrap(), r#"
{"width":679,"height":342,"viewRenderOptions":{"premultipliedAlpha":false},"data":[{"name":"backendChoropleth","format":"polys","sql":"SELECT ty_flights_mpoly_table.dest_state_mpoly AS dest_state_mpoly FROM ty_flights_mpoly_table WHERE (ST_XMax(ty_flights_mpoly_table.dest_state_mpoly) >= -125.65257161471237 AND ST_XMin(ty_flights_mpoly_table.dest_state_mpoly) <= -65.67742838528845 AND ST_YMax(ty_flights_mpoly_table.dest_state_mpoly) >= 25.820000000001016 AND ST_YMin(ty_flights_mpoly_table.dest_state_mpoly) <= 49.38000000000076 AND ((((origin_state_name = 'Tennessee')))) AND ST_XMax(origin_state_mpoly) >= -125.65257161471305\n          AND ST_XMin(origin_state_mpoly) <= -65.6774283852891\n          AND ST_YMax(origin_state_mpoly) >= 25.82000000000167\n          AND ST_YMin(origin_state_mpoly) <= 49.38000000000119) AND SAMPLE_RATIO(0.0027497656099794055)","enableHitTesting":true}],"scales":[{"name":"x","type":"linear","domain":[-13987580.287095958,-7311177.883458873],"range":"width"},{"name":"y","type":"linear","domain":[2976804.1982603935,6339587.146785449],"range":"height"}],"projections":[{"name":"mercator_map_projection","type":"mercator","bounds":{"x":[-125.65257161471237,-65.67742838528845],"y":[25.820000000001016,49.38000000000076]}}],"marks":[{"type":"polys","from":{"data":"backendChoropleth"},"properties":{"x":{"field":"x"},"y":{"field":"y"},"fillColor":{"value":"rgba(234,85,69,0.85)"},"strokeColor":"white","strokeWidth":1,"lineJoin":"miter","miterLimit":10},"transform":{"projection":"mercator_map_projection"}}]}
"#.trim());
    let name_values = rec.name_values.unwrap();
    assert_eq!(name_values[4], r#"nonce"#);
    assert_eq!(name_values[5], r#"15"#);
}

#[test]
fn render_vega_begin2() {
    let rec = pln(r#"
2020-07-01T00:00:00.000372 I 15 DBHandler.cpp:3358 stdlog_begin render_vega 13 0 omnisci admin 835-gX4x {"widget_id","compression_level","vega_json","nonce"} {"0","0","{""width"":1002,""height"":726,""viewRenderOptions"":{""premultipliedAlpha"":false},""data"":[{""name"":""pointmap"",""sql"":""SELECT conv_4326_900913_x(st_xmin(omnisci_geo)) AS x, conv_4326_900913_y(st_ymin(omnisci_geo )) AS y FROM omnisci_states WHERE ((st_xmin(omnisci_geo) is not null\n          AND st_ymin(omnisci_geo ) is not null\n          AND st_xmin(omnisci_geo) >= -178.12315200000032 AND st_xmin(omnisci_geo) <= -67.26987899999968 AND st_ymin(omnisci_geo ) >= -0.8144879012842097 AND st_ymin(omnisci_geo ) <= 61.96302517868901)) LIMIT 10000000"",""enableHitTesting"":false}],""scales"":[{""name"":""x"",""type"":""linear"",""domain"":[-19828578.576412328,-7488448.674977641],""range"":""width""},{""name"":""y"",""type"":""linear"",""domain"":[-90671.43229163112,8850380.771762503],""range"":""height""},{""name"":""pointmap_fillColor"",""type"":""linear"",""domain"":[0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1],""range"":[""rgba(17,95,154,0.475)"",""rgba(25,132,197,0.5471153846153846)"",""rgba(34,167,240,0.6192307692307691)"",""rgba(72,181,196,0.6913461538461538)"",""rgba(118,198,143,0.7634615384615384)"",""rgba(166,215,91,0.835576923076923)"",""rgba(201,229,47,0.85)"",""rgba(208,238,17,0.85)"",""rgba(208,244,0,0.85)""],""accumulator"":""density"",""minDensityCnt"":""-2ndStdDev"",""maxDensityCnt"":""2ndStdDev"",""clamp"":true}],""projections"":[],""marks"":[{""type"":""symbol"",""from"":{""data"":""pointmap""},""properties"":{""xc"":{""scale"":""x"",""field"":""x""},""yc"":{""scale"":""y"",""field"":""y""},""fillColor"":{""scale"":""pointmap_fillColor"",""value"":0},""shape"":""circle"",""width"":5,""height"":5}}]}","{""chartId"":""work_to_generate_logs"",""dashboardId"":100}"}
    "#);
    assert_eq!(rec.event.unwrap(), "render_vega_begin");
    // println!("{:}", rec.query.unwrap());
    assert_eq!(rec.query.unwrap(), r#"
{"width":1002,"height":726,"viewRenderOptions":{"premultipliedAlpha":false},"data":[{"name":"pointmap","sql":"SELECT conv_4326_900913_x(st_xmin(omnisci_geo)) AS x, conv_4326_900913_y(st_ymin(omnisci_geo )) AS y FROM omnisci_states WHERE ((st_xmin(omnisci_geo) is not null\n          AND st_ymin(omnisci_geo ) is not null\n          AND st_xmin(omnisci_geo) >= -178.12315200000032 AND st_xmin(omnisci_geo) <= -67.26987899999968 AND st_ymin(omnisci_geo ) >= -0.8144879012842097 AND st_ymin(omnisci_geo ) <= 61.96302517868901)) LIMIT 10000000","enableHitTesting":false}],"scales":[{"name":"x","type":"linear","domain":[-19828578.576412328,-7488448.674977641],"range":"width"},{"name":"y","type":"linear","domain":[-90671.43229163112,8850380.771762503],"range":"height"},{"name":"pointmap_fillColor","type":"linear","domain":[0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1],"range":["rgba(17,95,154,0.475)","rgba(25,132,197,0.5471153846153846)","rgba(34,167,240,0.6192307692307691)","rgba(72,181,196,0.6913461538461538)","rgba(118,198,143,0.7634615384615384)","rgba(166,215,91,0.835576923076923)","rgba(201,229,47,0.85)","rgba(208,238,17,0.85)","rgba(208,244,0,0.85)"],"accumulator":"density","minDensityCnt":"-2ndStdDev","maxDensityCnt":"2ndStdDev","clamp":true}],"projections":[],"marks":[{"type":"symbol","from":{"data":"pointmap"},"properties":{"xc":{"scale":"x","field":"x"},"yc":{"scale":"y","field":"y"},"fillColor":{"scale":"pointmap_fillColor","value":0},"shape":"circle","width":5,"height":5}}]}
    "#.trim());
    assert_eq!(rec.dashboardid.unwrap(), "100");
    assert_eq!(rec.chartid.unwrap(), "work_to_generate_logs");
}
