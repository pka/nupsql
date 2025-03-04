use nu_plugin::{serve_plugin, Plugin, PluginCommand, SimplePluginCommand};
use nu_protocol::{LabeledError, Record, Signature, Span, SyntaxShape, Value};
use postgres::{types::Type, Error};

struct Psql {}

impl Psql {
    fn new() -> Psql {
        Psql {}
    }

    fn cmd(&self, conn: &str, query: &str, span: Span) -> Result<Vec<Value>, LabeledError> {
        psql(conn, query, span).map_err(|e| LabeledError::new(e.to_string()))
    }
}

fn psql(connstr: &str, query: &str, span: Span) -> Result<Vec<Value>, Error> {
    let mut conn = postgres::Client::connect(connstr, postgres::NoTls)?;
    let stmt = conn.prepare(query)?;
    let columns = stmt.columns();

    let mut records = vec![];
    for row in conn.query(&stmt, &[])? {
        let mut record = Record::new();
        for (i, col) in columns.iter().enumerate() {
            let opt_value = match col.type_() {
                &Type::TEXT | &Type::VARCHAR => {
                    row.try_get::<_, String>(i).map(|s| Value::string(s, span))
                }
                &Type::INT2 => row.try_get::<_, i16>(i).map(|n| Value::int(n as i64, span)),
                &Type::INT4 => row.try_get::<_, i32>(i).map(|n| Value::int(n as i64, span)),
                &Type::INT8 => row.try_get::<_, i64>(i).map(|n| Value::int(n, span)),
                &Type::FLOAT4 => row
                    .try_get::<_, f32>(i)
                    .map(|f| Value::float(f as f64, span)),
                &Type::FLOAT8 => row.try_get::<_, f64>(i).map(|f| Value::float(f, span)),
                &Type::BOOL => row.try_get::<_, bool>(i).map(|b| Value::bool(b, span)),
                &Type::BYTEA => row.try_get::<_, Vec<u8>>(i).map(|b| Value::binary(b, span)),
                _ => Ok(Value::nothing(span)),
            }
            .unwrap_or_else(|_| Value::nothing(span));
            record.push(col.name(), opt_value);
        }
        records.push(Value::record(record, span));
    }
    Ok(records)
}

impl SimplePluginCommand for Psql {
    type Plugin = Psql;

    fn name(&self) -> &str {
        "psql"
    }

    fn description(&self) -> &str {
        "Execute PostgreSQL query."
    }

    fn signature(&self) -> Signature {
        Signature::build("psql")
            .description("Execute PostgreSQL query.")
            .named(
                "conn",
                SyntaxShape::String,
                "DB connection string",
                Some('c'),
            )
            .required("query", SyntaxShape::String, "SQL query")
    }

    fn run(
        &self,
        _plugin: &Self::Plugin,
        engine: &nu_plugin::EngineInterface,
        call: &nu_plugin::EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        let query = call.positional[0].as_str()?;
        let conn = call
            .get_flag_value("conn")
            .or_else(|| {
                engine
                    .get_plugin_config()
                    .ok()??
                    .get_data_by_key("DATABASE_URL")
            })
            .or_else(|| engine.get_env_var("DATABASE_URL").ok().flatten())
            .ok_or_else(|| LabeledError::new("missing both `--conn` and `DATABASE_URL`"))?;

        self.cmd(conn.as_str()?, query, call.head)
            .map(|table| Value::list(table, call.head))
            .map_err(Into::into)
    }
}

impl Plugin for Psql {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_owned()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![Box::new(Self::new())]
    }
}

fn main() {
    serve_plugin(&mut Psql::new(), nu_plugin::JsonSerializer);
}
