use arrow_schema::{DataType, SchemaRef};
use chrono::Days;
use datafusion::logical_expr::Expr;

use crate::DeltaSharingError;
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "op")]
#[serde(rename_all = "camelCase")]
pub enum Op {
    Column(ColumnOp),
    Literal(LiteralOp),
    IsNull(IsNullOp),
    Equal(Equal),
    LessThan(LessThan),
    LessThanOrEqual(LessThanOrEqual),
    GreaterThan(GreaterThan),
    GreaterThanOrEqual(GreaterThanOrEqual),
    And(And),
    Or(Or),
    Not(Not),
}

impl Op {
    pub fn column(name: &str, value_type: ValueType) -> Self {
        Op::Column(ColumnOp {
            name: name.to_string(),
            value_type,
        })
    }

    pub fn literal(value: impl Into<String>, value_type: ValueType) -> Self {
        Op::Literal(LiteralOp {
            value: value.into(),
            value_type,
        })
    }

    pub fn is_null(child: Op) -> Self {
        Op::IsNull(IsNullOp {
            children: vec![child],
        })
    }

    pub fn equal(left: Op, right: Op) -> Self {
        Op::Equal(Equal {
            children: vec![left, right],
        })
    }

    pub fn less_than(left: Op, right: Op) -> Self {
        Op::LessThan(LessThan {
            children: vec![left, right],
        })
    }

    pub fn less_than_or_equal(left: Op, right: Op) -> Self {
        Op::LessThanOrEqual(LessThanOrEqual {
            children: vec![left, right],
        })
    }

    pub fn greater_than(left: Op, right: Op) -> Self {
        Op::GreaterThan(GreaterThan {
            children: vec![left, right],
        })
    }

    pub fn greater_than_or_equal(left: Op, right: Op) -> Self {
        Op::GreaterThanOrEqual(GreaterThanOrEqual {
            children: vec![left, right],
        })
    }

    pub fn and(children: Vec<Op>) -> Self {
        Op::And(And { children })
    }

    pub fn or(children: Vec<Op>) -> Self {
        Op::Or(Or { children })
    }

    pub fn not(child: Op) -> Self {
        Op::Not(Not {
            children: vec![child],
        })
    }
}

impl Op {
    pub fn from_expr(expr: &Expr, schema: SchemaRef) -> Result<Self, DeltaSharingError> {
        let converted = match expr {
            Expr::Column(col) => {
                let name = &col.name;
                let value_type = schema
                    .field_with_name(name)
                    .map_err(|e| DeltaSharingError::other(e.to_string()))?
                    .data_type()
                    .try_into()?;
                Op::column(&name, value_type)
            }
            Expr::Literal(lit) => {
                let value_type = ValueType::try_from(&lit.data_type())?;
                match value_type {
                    ValueType::Date => {
                        let days: u64 = lit.to_string().parse().unwrap();
                        let value = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(Days::new(days))
                            .unwrap()
                            .format("%Y-%m-%d")
                            .to_string();
                        Op::literal(value, value_type)
                    }
                    _ => {
                        let value = lit.to_string();
                        Op::literal(value, value_type)
                    }
                }
            }
            Expr::BinaryExpr(bin) => {
                let left = Op::from_expr(&bin.left, schema.clone())?;
                let right = Op::from_expr(&bin.right, schema.clone())?;
                match bin.op {
                    datafusion::logical_expr::Operator::Eq => Op::equal(left, right),
                    datafusion::logical_expr::Operator::Lt => Op::less_than(left, right),
                    datafusion::logical_expr::Operator::LtEq => Op::less_than_or_equal(left, right),
                    datafusion::logical_expr::Operator::Gt => Op::greater_than(left, right),
                    datafusion::logical_expr::Operator::GtEq => {
                        Op::greater_than_or_equal(left, right)
                    }
                    datafusion::logical_expr::Operator::And => Op::and(vec![left, right]),
                    datafusion::logical_expr::Operator::Or => Op::or(vec![left, right]),
                    _ => unimplemented!(),
                }
            }
            Expr::Not(child) => {
                let child = Op::from_expr(child, schema)?;
                Op::not(child)
            }
            Expr::IsNotNull(_) => todo!(),
            Expr::IsNull(child) => {
                let child = Op::from_expr(child, schema)?;
                Op::is_null(child)
            }
            Expr::IsTrue(_) => todo!(),
            Expr::IsFalse(_) => todo!(),
            Expr::IsUnknown(_) => todo!(),
            Expr::IsNotTrue(_) => todo!(),
            Expr::IsNotFalse(_) => todo!(),
            Expr::IsNotUnknown(_) => todo!(),
            Expr::Between(_) => todo!(),
            _ => unimplemented!(),
        };

        Ok(converted)
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ValueType {
    Bool,
    Int,
    Long,
    String,
    Date,
    Float,
    Double,
    Timestamp,
}

impl TryFrom<&DataType> for ValueType {
    type Error = DeltaSharingError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        let converted = match value {
            DataType::Boolean => ValueType::Bool,
            DataType::Int32 => ValueType::Int,
            DataType::Int64 => ValueType::Long,
            DataType::Float32 => ValueType::Float,
            DataType::Float64 => ValueType::Double,
            DataType::Timestamp(_, _) => ValueType::Timestamp,
            DataType::Date32 => ValueType::Date,
            DataType::Utf8 => ValueType::String,
            _ => return Err(DeltaSharingError::other("Unsupported data type")),
        };
        Ok(converted)
    }
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ColumnOp {
    name: String,
    value_type: ValueType,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LiteralOp {
    value: String,
    value_type: ValueType,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct IsNullOp {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Equal {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct LessThan {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct LessThanOrEqual {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GreaterThan {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GreaterThanOrEqual {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct And {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Or {
    children: Vec<Op>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Not {
    children: Vec<Op>,
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::{Field, Schema};
    use datafusion::{prelude::*, scalar::ScalarValue};

    use super::*;

    #[test]
    fn equal_op_from_expr() {
        let column = col("`hireDate`");
        let literal = Expr::Literal(ScalarValue::Date32(Some(18746)));
        let expr = binary_expr(column, datafusion::logical_expr::Operator::Eq, literal);
        let schema = Schema::new(vec![Field::new("hireDate", DataType::Date32, false)]);

        let parsed_op = Op::from_expr(&expr, Arc::new(schema)).unwrap();
        let expected_op = Op::equal(
            Op::column("hireDate", ValueType::Date),
            Op::literal("2021-04-29", ValueType::Date),
        );
        assert_eq!(parsed_op, expected_op);

        let parsed_filter = parsed_op.to_string();
        let expected_filter = r#"{"op":"equal","children":[{"op":"column","name":"hireDate","valueType":"date"},{"op":"literal","value":"2021-04-29","valueType":"date"}]}"#;
        assert_eq!(parsed_filter, expected_filter);
    }

    #[test]
    fn and_op_from_expr() {
        let eq_column = col("`hireDate`");
        let eq_literal = Expr::Literal(ScalarValue::Date32(Some(18746)));
        let eq_expr = binary_expr(
            eq_column,
            datafusion::logical_expr::Operator::Eq,
            eq_literal,
        );

        let lt_column = col("`id`");
        let lt_literal = Expr::Literal(ScalarValue::Int32(Some(25)));
        let lt_expr = binary_expr(
            lt_column,
            datafusion::logical_expr::Operator::Lt,
            lt_literal,
        );

        let expr = binary_expr(eq_expr, datafusion::logical_expr::Operator::And, lt_expr);
        let schema = Schema::new(vec![
            Field::new("hireDate", DataType::Date32, true),
            Field::new("id", DataType::Int32, false),
        ]);

        let parsed_op = Op::from_expr(&expr, Arc::new(schema)).unwrap();
        let expected_op = Op::and(vec![
            Op::equal(
                Op::column("hireDate", ValueType::Date),
                Op::literal("2021-04-29", ValueType::Date),
            ),
            Op::less_than(
                Op::column("id", ValueType::Int),
                Op::literal("25", ValueType::Int),
            ),
        ]);
        assert_eq!(parsed_op, expected_op);

        let parsed_filter = parsed_op.to_string();
        let expected_filter = r#"{"op":"and","children":[{"op":"equal","children":[{"op":"column","name":"hireDate","valueType":"date"},{"op":"literal","value":"2021-04-29","valueType":"date"}]},{"op":"lessThan","children":[{"op":"column","name":"id","valueType":"int"},{"op":"literal","value":"25","valueType":"int"}]}]}"#;
        assert_eq!(parsed_filter, expected_filter);
    }

    #[test]
    fn not_op_from_expr() {
        let column = col("`id`");
        let expr = Expr::Not(Box::new(Expr::IsNull(Box::new(column))));
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let parsed_op = Op::from_expr(&expr, Arc::new(schema)).unwrap();
        let expected_op = Op::not(Op::is_null(Op::column("id", ValueType::Int)));
        assert_eq!(parsed_op, expected_op);

        let parsed_filter = parsed_op.to_string();
        let expected_filter = r#"{"op":"not","children":[{"op":"isNull","children":[{"op":"column","name":"id","valueType":"int"}]}]}"#;
        assert_eq!(parsed_filter, expected_filter);
    }
}
