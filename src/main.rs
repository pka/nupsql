use futures::executor::block_on;
use nu_errors::ShellError;
use nu_protocol::{
    serve_plugin, CallInfo, Plugin, Primitive, ReturnSuccess, ReturnValue, Signature, SyntaxShape,
    TaggedDictBuilder, UntaggedValue, Value,
};
use nu_source::Tag;
use tokio_postgres::{types::Type, Error, NoTls, Row};

struct Psql {
    conn: Option<String>,
    query: Option<String>,
}

impl Psql {
    fn new() -> Psql {
        Psql {
            conn: None,
            query: None,
        }
    }

    fn cmd(&mut self, tag: Tag) -> Result<Vec<Value>, ShellError> {
        block_on(psql(
            self.conn.as_ref().unwrap(),
            self.query.as_ref().unwrap(),
            tag,
        ))
        .map_err(|e| ShellError::untagged_runtime_error(format!("{}", e)))
    }
}

async fn psql(connstr: &str, query: &str, tag: Tag) -> Result<Vec<Value>, Error> {
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let stmt = client.prepare(query).await?;
    let columns = stmt.columns();

    let mut records = vec![];
    let rows: Vec<Row> = client.query(&stmt, &[]).await?;
    for row in rows {
        let mut dict = TaggedDictBuilder::new(&tag);
        for (i, col) in columns.iter().enumerate() {
            let opt_value = match col.type_() {
                &Type::TEXT | &Type::VARCHAR => {
                    row.try_get::<_, &str>(i).map(UntaggedValue::string)
                }
                &Type::INT2 => row.try_get::<_, i16>(i).map(UntaggedValue::int),
                &Type::INT4 => row.try_get::<_, i32>(i).map(UntaggedValue::int),
                &Type::INT8 => row.try_get::<_, i64>(i).map(UntaggedValue::int),
                &Type::FLOAT4 => row.try_get::<_, f32>(i).map(UntaggedValue::decimal),
                &Type::FLOAT8 => row.try_get::<_, f64>(i).map(UntaggedValue::decimal),
                // &Type::NUMERIC => row.try_get::<_, f64>(i).map(UntaggedValue::decimal),
                &Type::BOOL => row.try_get::<_, bool>(i).map(UntaggedValue::boolean),
                // &Type::DATE | &Type::TIME | &Type::TIMESTAMP | &Type::TIMESTAMPTZ =>
                &Type::BYTEA => row.try_get::<_, Vec<u8>>(i).map(UntaggedValue::binary),
                _ => Ok(UntaggedValue::nothing()),
            };
            dict.insert_untagged(col.name(), opt_value.unwrap_or(UntaggedValue::nothing()));
        }
        records.push(dict.into_value());
    }
    Ok(records)
}

impl Plugin for Psql {
    fn config(&mut self) -> Result<Signature, ShellError> {
        Ok(Signature::build("psql")
            .desc("Execute PostgreSQL query.")
            .required("conn", SyntaxShape::String, "DB connection string")
            .required("query", SyntaxShape::String, "SQL query")
            // .rest(SyntaxShape::String)
            .filter())
    }

    fn begin_filter(&mut self, call_info: CallInfo) -> Result<Vec<ReturnValue>, ShellError> {
        if let Some(args) = call_info.args.positional {
            match &args[0] {
                Value {
                    value: UntaggedValue::Primitive(Primitive::String(s)),
                    ..
                } => {
                    self.conn = Some(s.clone());
                }
                _ => {
                    return Err(ShellError::untagged_runtime_error(format!(
                        "Unrecognized type in params: {:?}",
                        args[0]
                    )))
                }
            }
            match &args[1] {
                Value {
                    value: UntaggedValue::Primitive(Primitive::String(s)),
                    ..
                } => {
                    self.query = Some(s.clone());
                }
                _ => {
                    return Err(ShellError::untagged_runtime_error(format!(
                        "Unrecognized type in params: {:?}",
                        args[1]
                    )))
                }
            }
        }

        self.cmd(call_info.name_tag)
            .map(|table| table.into_iter().map(ReturnSuccess::value).collect())
    }
}

#[tokio::main]
async fn main() {
    serve_plugin(&mut Psql::new());
}
