use futures::executor::block_on;
use futures::{FutureExt, TryStreamExt};
use nu::{
    serve_plugin, CallInfo, Plugin, Primitive, ReturnSuccess, ReturnValue, ShellError, Signature,
    SyntaxShape, Tag, Tagged, TaggedDictBuilder, Value,
};
use tokio_postgres::{Error, NoTls, Row};

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

    // fn filter(&mut self, value: Tagged<Value>) -> Result<Tagged<Value>, ShellError> {
    //     match value.item {
    //         Value::Primitive(Primitive::String(_s)) => Ok(Tagged {
    //             item: block_on(self.psql("host=localhost user=postgres".to_string()))
    //                 .map_err(|e| ShellError::string(format!("{}", e)))?,
    //             tag: value.tag,
    //         }),
    //         _ => Err(ShellError::labeled_error(
    //             "Unrecognized type in stream",
    //             "'psql' given non-string by this",
    //             value.tag.span,
    //         )),
    //     }
    // }

    fn cmd(&mut self, tag: Tag) -> Vec<Tagged<Value>> {
        let mut output = vec![];
        let mut dict = TaggedDictBuilder::new(tag);
        let res = block_on(self.psql(self.conn.as_ref().unwrap().to_string())).unwrap();
        dict.insert("res", res);
        output.push(dict.into_tagged_value());
        output
    }

    async fn psql(&mut self, connstr: String) -> Result<Value, Error> {
        // Connect to the database.
        let (mut client, connection) = tokio_postgres::connect(&connstr, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection = connection.map(|r| {
            if let Err(e) = r {
                eprintln!("connection error: {}", e);
            }
        });
        tokio::spawn(connection);

        // Now we can prepare a simple statement that just returns its parameter.
        let stmt = client.prepare(self.query.as_ref().unwrap()).await?;

        // And then execute it, returning a Stream of Rows which we collect into a Vec.
        let rows: Vec<Row> = client.query(&stmt, &[]).try_collect().await?;

        // Now we can check that we got back the same string we sent over.
        let value: &str = rows[0].get(0);
        Ok(Value::string(value))
    }
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

        Ok(self
            .cmd(call_info.name_tag)
            .into_iter()
            .map(ReturnSuccess::value)
            .collect())
    }
}

#[tokio::main]
async fn main() {
    serve_plugin(&mut Psql::new());
}
