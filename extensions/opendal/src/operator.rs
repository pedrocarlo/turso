use opendal::{layers::TracingLayer, services, Operator, Result};

struct LimboOperator {
    op: Operator,
}

impl LimboOperator {
    pub fn new() -> Result<Self> {
        // Pick a builder and configure it.
        let builder = services::S3::default().bucket("test");

        // Init an operator
        let op = Operator::new(builder)?.layer(TracingLayer).finish();
        Ok(LimboOperator { op })
    }

}
