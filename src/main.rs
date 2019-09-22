use futures::{Future, Stream};
use nu::{
    serve_plugin, CallInfo, Plugin, Primitive, ReturnSuccess, ReturnValue, ShellError, Signature,
    Tagged, Value,
};
use tokio_postgres::NoTls;

struct Psql;

impl Psql {
    fn new() -> Psql {
        Psql
    }

    fn cmd(&mut self, value: Tagged<Value>) -> Result<Tagged<Value>, ShellError> {
        match value.item {
            Value::Primitive(Primitive::String(_s)) => Ok(Tagged {
                item: self.psql("host=localhost user=postgres".to_string()),
                tag: value.tag,
            }),
            _ => Err(ShellError::labeled_error(
                "Unrecognized type in stream",
                "'psql' given non-string by this",
                value.tag.span,
            )),
        }
    }

    fn psql(&mut self, connstr: String) -> Value {
        let fut =
        // Connect to the database
        tokio_postgres::connect(&connstr, NoTls)

        .map(|(client, connection)| {
            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
            tokio::spawn(connection);

            // The client is what you use to make requests.
            client
        })

        .and_then(|mut client| {
            // Now we can prepare a simple statement that just returns its parameter.
            client.prepare("SELECT $1::TEXT")
                .map(|statement| (client, statement))
        })

        .and_then(|(mut client, statement)| {
            // And then execute it, returning a Stream of Rows which we collect into a Vec
            client.query(&statement, &[&"hello world"]).collect()
        })

        // Now we can check that we got back the same string we sent over.
        .map(|rows| {
            let value: &str = rows[0].get(0);
            Value::string(value)
        })

        // And report any errors that happened.
        .map_err(|e| {
            eprintln!("error: {}", e);
        });

        // By default, tokio_postgres uses the tokio crate as its runtime.
        let mut runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime.block_on(fut).unwrap()
    }
}

impl Plugin for Psql {
    fn config(&mut self) -> Result<Signature, ShellError> {
        Ok(Signature::build("psql").desc("Execute PostgreSQL query."))
    }

    fn begin_filter(&mut self, _: CallInfo) -> Result<Vec<ReturnValue>, ShellError> {
        Ok(vec![])
    }

    fn filter(&mut self, input: Tagged<Value>) -> Result<Vec<ReturnValue>, ShellError> {
        Ok(vec![ReturnSuccess::value(self.cmd(input)?)])
    }
}

fn main() {
    serve_plugin(&mut Psql::new());
}
