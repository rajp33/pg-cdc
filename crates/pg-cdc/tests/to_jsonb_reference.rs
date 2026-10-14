use pg_cdc::catalog::TypeResolver;
use pg_cdc_decoder::message::{LogicalMessage, TupleValue, decode};
use pg_cdc_decoder::value::map_value;
use pg_cdc_testkit::Postgres;
use serde_json::Value;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const TO_JSONB: &str = "to_jsonb({c})";
const AS_TEXT: &str = "to_jsonb({c}::text)";
const AS_TEXT_ARRAY: &str = "to_jsonb({c}::text[])";
const AS_INT8: &str = "to_jsonb({c}::int8)";

struct Case {
    sql_type: &'static str,
    expected: &'static str,
    literals: &'static [&'static str],
}

const fn case(sql_type: &'static str, literals: &'static [&'static str]) -> Case {
    Case {
        sql_type,
        expected: TO_JSONB,
        literals,
    }
}

const CASES: &[Case] = &[
    case("bool", &["true", "false"]),
    case("int2", &["-32768", "32767"]),
    case("int4", &["-2147483648", "0"]),
    case("int8", &["-9223372036854775808", "9223372036854775807"]),
    Case {
        sql_type: "oid",
        expected: AS_INT8,
        literals: &["0", "4294967295"],
    },
    case(
        "float4",
        &[
            "'NaN'",
            "'Infinity'",
            "'-Infinity'",
            "'0.1'",
            "'-0'",
            "'3.4028235e38'",
            "'1.5e-7'",
        ],
    ),
    case(
        "float8",
        &[
            "'NaN'",
            "'-Infinity'",
            "'1e100'",
            "'5e-324'",
            "'0.1'",
            "'-0'",
            "'123456789012345680000'",
        ],
    ),
    case(
        "numeric",
        &[
            "'NaN'",
            "'Infinity'",
            "'-Infinity'",
            "'1.50'",
            "'-12.3456'",
            "'0.000000000001'",
            "'1e30'",
        ],
    ),
    case(
        "text",
        &[
            "''",
            r#"'quote " and backslash \'"#,
            "'emoji 🐘'",
            r"E'tab\tnewline\n'",
        ],
    ),
    case("varchar(10)", &["'abc'"]),
    case("bpchar(5)", &["'ab'"]),
    case("name", &["'pg_catalog'"]),
    case("\"char\"", &["'a'"]),
    case(
        "json",
        &[
            r#"'{"b": 1, "a": 1e2, "a": 3}'"#,
            r#"'[1.0, -0, "x"]'"#,
            "'null'",
        ],
    ),
    case(
        "jsonb",
        &[r#"'{"x": 1.0e2, "nested": {"k": [true, null]}}'"#],
    ),
    case(
        "date",
        &[
            "'2026-10-14'",
            "'infinity'",
            "'0044-03-15 BC'",
            "'10000-01-01'",
        ],
    ),
    case(
        "timestamp",
        &[
            "'2026-10-14 12:00:00.5'",
            "'-infinity'",
            "'0044-03-15 12:00 BC'",
        ],
    ),
    case(
        "timestamptz",
        &[
            "'2026-10-14 12:00:00.123456+00'",
            "'infinity'",
            "'2026-10-14 08:00:00-04'",
            "'0044-03-15 12:00+00 BC'",
        ],
    ),
    case("time", &["'12:34:56.789'"]),
    case("timetz", &["'12:34:56+05:30'"]),
    case("interval", &["'1 day 02:03:04'", "'-1 year'"]),
    case("bytea", &[r"'\x0102ff'", "''"]),
    case("uuid", &["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"]),
    case("inet", &["'192.168.0.1/24'"]),
    case("cidr", &["'10.0.0.0/8'"]),
    case("macaddr", &["'08:00:2b:01:02:03'"]),
    case("macaddr8", &["'08:00:2b:01:02:03:04:05'"]),
    case("money", &["'12.34'"]),
    case("bit(4)", &["B'1010'"]),
    case("varbit", &["B'101'"]),
    case("xml", &["'<a>1</a>'"]),
    case("tsvector", &["'a fat cat'"]),
    case("tsquery", &["'fat & cat'"]),
    case("point", &["'(1,2)'"]),
    case("line", &["'{1,2,3}'"]),
    case("lseg", &["'[(0,0),(1,1)]'"]),
    case("box", &["'(1,1),(0,0)'"]),
    case("path", &["'[(0,0),(1,1)]'"]),
    case("polygon", &["'((0,0),(1,1),(1,0))'"]),
    case("circle", &["'<(0,0),1>'"]),
    case("int4range", &["'[1,5)'", "'empty'"]),
    case("numrange", &["'(1.5,2.5]'"]),
    case("tstzrange", &["'[2026-10-14 00:00+00,)'"]),
    case("daterange", &["'[2026-01-01,2026-02-01)'"]),
    case("int4multirange", &["'{[1,3),[5,7)}'"]),
    case("pg_lsn", &["'0/1A2B3C4D'"]),
    case("jsonpath", &["'$.a[*] ? (@ > 1)'"]),
    case(
        "int4[]",
        &["'{1,NULL,3}'", "'[0:1]={1,2}'", "'{}'", "'{{1,2},{3,4}}'"],
    ),
    case(
        "text[]",
        &[r#"'{a,"b c","NULL",NULL,"q\"uote","back\\slash",""}'"#],
    ),
    case("bool[]", &["'{t,f}'"]),
    case("float8[]", &["'{NaN,1.5,1e100}'"]),
    case("numeric[]", &["'{1.50,NaN}'"]),
    case("bytea[]", &[r#"'{"\\x01",""}'"#]),
    case("date[]", &["'{2026-10-14,infinity}'"]),
    case("timestamptz[]", &[r#"'{"2026-10-14 12:00:00+00"}'"#]),
    case("jsonb[]", &[r#"'{"{\"a\": 1}"}'"#]),
    case("uuid[]", &["'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'"]),
    case("box[]", &["'{(1,1),(0,0);(2,2),(1,1)}'"]),
    case("mood", &["'happy'"]),
    case("mood[]", &["'{happy,sad}'"]),
    case("posint", &["5"]),
    case("posint[]", &["'{1,2}'"]),
    Case {
        sql_type: "pair",
        expected: AS_TEXT,
        literals: &["ROW(1, 'x y')"],
    },
    Case {
        sql_type: "pair[]",
        expected: AS_TEXT_ARRAY,
        literals: &["ARRAY[ROW(1, 'x y')::pair]"],
    },
];

const SETUP: &str = "CREATE TYPE mood AS ENUM ('happy', 'sad');
     CREATE DOMAIN posint AS int CHECK (VALUE > 0);
     CREATE TYPE pair AS (a int, b text);";

fn rows() -> usize {
    CASES
        .iter()
        .map(|case| case.literals.len())
        .max()
        .unwrap_or(0)
}

fn create_table() -> String {
    let columns: Vec<String> = CASES
        .iter()
        .enumerate()
        .map(|(i, case)| format!("c{i} {}", case.sql_type))
        .collect();
    format!(
        "CREATE TABLE ref_values (id int PRIMARY KEY, {})",
        columns.join(", ")
    )
}

fn inserts() -> String {
    let statements: Vec<String> = (0..rows())
        .map(|row| {
            let values: Vec<&str> = CASES
                .iter()
                .map(|case| case.literals.get(row).copied().unwrap_or("NULL"))
                .collect();
            format!(
                "INSERT INTO ref_values VALUES ({row}, {});",
                values.join(", ")
            )
        })
        .collect();
    statements.join("\n")
}

fn expected_query() -> String {
    let columns: Vec<String> = CASES
        .iter()
        .enumerate()
        .map(|(i, case)| format!("{}::text", case.expected.replace("{c}", &format!("c{i}"))))
        .collect();
    format!("SELECT {} FROM ref_values ORDER BY id", columns.join(", "))
}

#[tokio::test]
async fn d2_to_jsonb_reference_matches_every_builtin_type() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute(SETUP).await?;
    pg.execute(&create_table()).await?;
    pg.create_publication("ref_pub", &["ref_values"]).await?;
    pg.create_slot("ref_slot").await?;
    pg.execute(&inserts()).await?;

    let client = pg.client().await?;
    let mut expected_rows = Vec::new();
    for row in client.query(&expected_query(), &[]).await? {
        let mut values = Vec::new();
        for index in 0..CASES.len() {
            let text: Option<String> = row.try_get(index)?;
            values.push(match text {
                Some(text) => serde_json::from_str::<Value>(&text)?,
                None => Value::Null,
            });
        }
        expected_rows.push(values);
    }

    let mut resolver = TypeResolver::new();
    let mut relation = None;
    let mut actual_rows = Vec::new();
    for bytes in pg.capture_pgoutput("ref_slot", "ref_pub").await? {
        match decode(&bytes)? {
            LogicalMessage::Relation(message) => relation = Some(message),
            LogicalMessage::Insert(insert) => {
                let relation = relation.as_ref().ok_or("insert before relation")?;
                let mut values = Vec::new();
                for (column, value) in relation.columns.iter().zip(insert.new.0).skip(1) {
                    values.push(match value {
                        TupleValue::Text(text) => {
                            let kind = resolver.resolve(&client, column.type_oid).await?;
                            let mapped = map_value(&kind, &text);
                            assert!(!mapped.fallback, "{} {text:?} fell back", column.name);
                            mapped.value
                        }
                        TupleValue::Null => Value::Null,
                        TupleValue::UnchangedToast => {
                            return Err("unexpected unchanged TOAST".into());
                        }
                    });
                }
                actual_rows.push(values);
            }
            _ => {}
        }
    }

    assert_eq!(actual_rows.len(), expected_rows.len());
    for (row, (actual, expected)) in actual_rows.iter().zip(&expected_rows).enumerate() {
        for (index, case) in CASES.iter().enumerate() {
            assert_eq!(
                actual[index], expected[index],
                "row {row}, {} ({})",
                case.sql_type, case.expected
            );
        }
    }
    Ok(())
}
