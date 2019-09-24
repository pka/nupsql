use futures::executor::block_on;
use futures::{FutureExt, TryStreamExt};
use nu::{
    serve_plugin, CallInfo, Plugin, Primitive, ReturnSuccess, ReturnValue, ShellError, Signature,
    SyntaxShape, Tag, Tagged, TaggedDictBuilder, Value,
};
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

    fn cmd(&mut self, tag: Tag) -> Result<Vec<Tagged<Value>>, ShellError> {
        block_on(psql(
            self.conn.as_ref().unwrap(),
            self.query.as_ref().unwrap(),
            tag,
        ))
        .map_err(|e| ShellError::string(format!("{}", e)))
    }
}

async fn psql(connstr: &str, query: &str, tag: Tag) -> Result<Vec<Tagged<Value>>, Error> {
    let (mut client, connection) = tokio_postgres::connect(&connstr, NoTls).await?;
    let connection = connection.map(|r| {
        if let Err(e) = r {
            eprintln!("connection error: {}", e);
        }
    });
    tokio::spawn(connection);

    let stmt = client.prepare(query).await?;
    let columns = stmt.columns();

    let mut records = vec![];
    let rows: Vec<Row> = client.query(&stmt, &[]).try_collect().await?;
    for row in rows {
        let mut dict = TaggedDictBuilder::new(tag);
        for (i, col) in columns.iter().enumerate() {
            let opt_value = match col.type_() {
                &Type::TEXT | &Type::VARCHAR => row.try_get::<_, &str>(i).map(Value::string),
                &Type::INT2 => row.try_get::<_, i16>(i).map(Value::int),
                &Type::INT4 => row.try_get::<_, i32>(i).map(Value::int),
                &Type::INT8 => row.try_get::<_, i64>(i).map(Value::int),
                &Type::FLOAT4 => row.try_get::<_, f32>(i).map(Value::decimal),
                &Type::FLOAT8 => row.try_get::<_, f64>(i).map(Value::decimal),
                // &Type::NUMERIC => row.try_get::<_, f64>(i).map(Value::decimal),
                &Type::BOOL => row.try_get::<_, bool>(i).map(Value::boolean),
                // &Type::DATE | &Type::TIME | &Type::TIMESTAMP | &Type::TIMESTAMPTZ =>
                &Type::BYTEA => row.try_get::<_, Vec<u8>>(i).map(Value::binary),
                _ => Ok(Value::nothing()),
            };
            dict.insert(col.name(), opt_value.unwrap_or(Value::nothing()));
        }
        records.push(dict.into_tagged_value());
    }
    Ok(records)
}

impl Plugin for Psql {
    fn config(&mut self) -> Result<Signature, ShellError> {
        Ok(Signature::build("psql")
            .desc("Execute PostgreSQL query.")
            .required("conn", SyntaxShape::String)
            .required("query", SyntaxShape::String)
            // .rest(SyntaxShape::String)
            .filter())
    }

    fn begin_filter(&mut self, call_info: CallInfo) -> Result<Vec<ReturnValue>, ShellError> {
        if let Some(args) = call_info.args.positional {
            match &args[0] {
                Tagged {
                    item: Value::Primitive(Primitive::String(s)),
                    ..
                } => {
                    self.conn = Some(s.clone());
                }
                _ => {
                    return Err(ShellError::string(format!(
                        "Unrecognized type in params: {:?}",
                        args[0]
                    )))
                }
            }
            match &args[1] {
                Tagged {
                    item: Value::Primitive(Primitive::String(s)),
                    ..
                } => {
                    self.query = Some(s.clone());
                }
                _ => {
                    return Err(ShellError::string(format!(
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
