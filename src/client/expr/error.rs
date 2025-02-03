use arrow_schema::{ArrowError, SchemaRef};

#[derive(Debug)]
pub enum ParseExpressionError {
    UnsupportedExpression {
        expression_name: String,
    },
    UnsupportedColumnDataType {
        column_name: String,
        data_type: String,
    },
    ColumnNotFound {
        column_name: String,
        schema: String,
        error: ArrowError,
    },
    BinaryOperationDoesNotSupportNonLeaf,
    IsNullCanOnlyBeAppliedToColumns,
}

impl ParseExpressionError {
    pub fn unsupported_expression(expression_name: String) -> Self {
        ParseExpressionError::UnsupportedExpression { expression_name }
    }

    pub fn unsupported_column_data_type(data_type: String) -> Self {
        ParseExpressionError::UnsupportedColumnDataType {
            column_name: String::from("unknown"),
            data_type,
        }
    }

    pub fn column_not_found(column_name: String, schema: SchemaRef, error: ArrowError) -> Self {
        ParseExpressionError::ColumnNotFound {
            column_name,
            schema: schema.to_string(),
            error,
        }
    }
}
