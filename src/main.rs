use nu_errors::ShellError;
use nu_plugin::{serve_plugin, Plugin};
use nu_protocol::{
    CallInfo, Primitive, ReturnSuccess, ReturnValue, Signature, SyntaxShape, TaggedDictBuilder,
    UntaggedValue, Value,
};
use nu_source::Tag;
use postgres::{types, Connection, Error, TlsMode};

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
        psql(
            self.conn.as_ref().unwrap(),
            self.query.as_ref().unwrap(),
            tag,
        )
        .map_err(|e| ShellError::untagged_runtime_error(format!("{}", e)))
    }
}

fn psql(connstr: &str, query: &str, tag: Tag) -> Result<Vec<Value>, Error> {
    let conn = Connection::connect(connstr, TlsMode::None)?;
    let stmt = conn.prepare(query)?;
    let columns = stmt.columns();

    let mut records = vec![];
    for row in &stmt.query(&[])? {
        let mut dict = TaggedDictBuilder::new(&tag);
        for (i, col) in columns.iter().enumerate() {
            let opt_value = match col.type_() {
                &types::TEXT | &types::VARCHAR => row
                    .get_opt::<_, String>(i)
                    .map(|opt| opt.map(UntaggedValue::string)),
                &types::INT2 => row
                    .get_opt::<_, i16>(i)
                    .map(|opt| opt.map(UntaggedValue::int)),
                &types::INT4 => row
                    .get_opt::<_, i32>(i)
                    .map(|opt| opt.map(UntaggedValue::int)),
                &types::INT8 => row
                    .get_opt::<_, i64>(i)
                    .map(|opt| opt.map(UntaggedValue::int)),
                &types::FLOAT4 => row
                    .get_opt::<_, f32>(i)
                    .map(|opt| opt.map(UntaggedValue::decimal)),
                &types::FLOAT8 => row
                    .get_opt::<_, f64>(i)
                    .map(|opt| opt.map(UntaggedValue::decimal)),
                // &types::NUMERIC => row
                //     .get_opt::<_, f64>(i)
                //     .map(|opt| opt.map(UntaggedValue::decimal)),
                &types::BOOL => row
                    .get_opt::<_, bool>(i)
                    .map(|opt| opt.map(UntaggedValue::boolean)),
                // &types::DATE | &types::TIME | &types::TIMESTAMP | &types::TIMESTAMPTZ =>
                &types::BYTEA => row
                    .get_opt::<_, Vec<u8>>(i)
                    .map(|opt| opt.map(UntaggedValue::binary)),
                _ => Some(Ok(UntaggedValue::nothing())),
            }
            .unwrap_or(Ok(UntaggedValue::nothing()))
            .unwrap_or(UntaggedValue::nothing());
            dict.insert_untagged(col.name(), opt_value);
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

fn main() {
    serve_plugin(&mut Psql::new());
}
