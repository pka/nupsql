use futures::executor::block_on;
use futures::{FutureExt, TryStreamExt};
use nu::{
    serve_plugin, CallInfo, Plugin, Primitive, ReturnSuccess, ReturnValue, ShellError, Signature,
    Tagged, Value,
};
use tokio_postgres::{Error, NoTls, Row};

struct Psql;

impl Psql {
    fn new() -> Psql {
        Psql
    }

    fn cmd(&mut self, value: Tagged<Value>) -> Result<Tagged<Value>, ShellError> {
        match value.item {
            Value::Primitive(Primitive::String(_s)) => Ok(Tagged {
                item: block_on(self.psql("host=localhost user=postgres".to_string()))
                    .map_err(|e| ShellError::string(format!("{}", e)))?,
                tag: value.tag,
            }),
            _ => Err(ShellError::labeled_error(
                "Unrecognized type in stream",
                "'psql' given non-string by this",
                value.tag.span,
            )),
        }
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
        let stmt = client.prepare("SELECT $1::TEXT").await?;

        // And then execute it, returning a Stream of Rows which we collect into a Vec.
        let rows: Vec<Row> = client.query(&stmt, &[&"hello world"]).try_collect().await?;

        // Now we can check that we got back the same string we sent over.
        let value: &str = rows[0].get(0);
        Ok(Value::string(value))
    }
}

impl Plugin for Psql {
    fn config(&mut self) -> Result<Signature, ShellError> {
        Ok(Signature::build("psql")
            .desc("Execute PostgreSQL query.")
            .filter())
    }

    fn begin_filter(&mut self, _: CallInfo) -> Result<Vec<ReturnValue>, ShellError> {
        Ok(vec![])
    }

    fn filter(&mut self, input: Tagged<Value>) -> Result<Vec<ReturnValue>, ShellError> {
        Ok(vec![ReturnSuccess::value(self.cmd(input)?)])
    }
}

#[tokio::main]
async fn main() {
    serve_plugin(&mut Psql::new());
}
