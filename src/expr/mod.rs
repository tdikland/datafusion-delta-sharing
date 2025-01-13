use std::ops::{BitAnd, BitOr, Not};

use arrow_schema::{DataType, SchemaRef};
use chrono::Days;
use datafusion::{logical_expr::Expr, scalar::ScalarValue};
use delta_kernel::actions::Add;

use crate::DeltaSharingError;
use error::ParseExpressionError;
use serde::{ser::SerializeStruct, Serialize};

pub(crate) mod error;

#[derive(Debug, PartialEq, Eq)]
pub enum Op {
    Column(Column),
    Literal(Literal),
    IsNull(Box<Op>),
    Equal { left: Box<Op>, right: Box<Op> },
    LessThan { left: Box<Op>, right: Box<Op> },
    LessThanOrEqual { left: Box<Op>, right: Box<Op> },
    GreaterThan { left: Box<Op>, right: Box<Op> },
    GreaterThanOrEqual { left: Box<Op>, right: Box<Op> },
    And(Vec<Op>),
    Or(Vec<Op>),
    Not(Box<Op>),
}

impl Op {
    /// Represents a column. A column op has two fields: name and valueType. The column's value will
    /// be cast to the specified valueType during comparisons.
    pub fn col(name: impl Into<String>, value_type: ValueType) -> Self {
        Op::Column(Column {
            name: name.into(),
            value_type,
        })
    }

    /// Represents a literal or fixed value. A literal op has two fields: value and valueType. The
    /// literal's value will be cast to the specified valueType during comparisons.
    pub fn lit(value: impl Into<String>, value_type: ValueType) -> Self {
        Op::Literal(Literal {
            value: value.into(),
            value_type,
        })
    }

    /// Represents a null check on a column op. This op should have only one child, the column op.
    pub fn is_null(child: Op) -> Self {
        debug_assert!(child.is_column());
        Op::IsNull(Box::new(child))
    }

    /// Represents an equality ("=") check. This op should have two children, and both should be
    /// leaf ops.
    pub fn eq(left: Op, right: Op) -> Self {
        debug_assert!(left.is_leaf() && right.is_leaf());
        Op::Equal {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Represents a less than ("<") check. This op should have two children, and both should be
    /// leaf ops.
    pub fn less_than(left: Op, right: Op) -> Self {
        debug_assert!(left.is_leaf() && right.is_leaf());
        Op::LessThan {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Represents a less than or equal ("<=") check. This op should have two children, and both
    /// should be leaf ops.
    pub fn less_than_or_equal(left: Op, right: Op) -> Self {
        debug_assert!(left.is_leaf() && right.is_leaf());
        Op::LessThanOrEqual {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Represents a greater than (">") check. This op should have two children, and both should be
    /// leaf ops.
    pub fn greater_than(left: Op, right: Op) -> Self {
        debug_assert!(left.is_leaf() && right.is_leaf());
        Op::GreaterThan {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Represents a greater than (">=") check. This op should have two children, and both should be
    /// leaf ops.
    pub fn greater_than_or_equal(left: Op, right: Op) -> Self {
        debug_assert!(left.is_leaf() && right.is_leaf());
        Op::GreaterThanOrEqual {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Represents a logical and operation amongst its children. This op should have at least two
    /// children.
    pub fn and(children: Vec<Op>) -> Self {
        debug_assert!(children.len() >= 2);
        let children = children
            .into_iter()
            .flat_map(|op| match op {
                Op::And(nested) => nested,
                op => vec![op],
            })
            .collect();

        Op::And(children)
    }

    /// Represents a logical or operation amongst its children. This op should have at least two
    /// children.
    pub fn or(children: Vec<Op>) -> Self {
        debug_assert!(children.len() >= 2);
        let children = children
            .into_iter()
            .flat_map(|op| match op {
                Op::Or(nested) => nested,
                op => vec![op],
            })
            .collect();
        Op::Or(children)
    }

    /// Represents a logical not check. This op should have one child.
    pub fn not(child: Op) -> Self {
        Op::Not(Box::new(child))
    }
}

impl Op {
    /// Check if the OpType is `Leaf`
    fn is_leaf(&self) -> bool {
        match self {
            Self::Column(_) | Self::Literal(_) => true,
            _ => false,
        }
    }

    /// Check if the Op is the `Column` variant
    fn is_column(&self) -> bool {
        matches!(self, Self::Column(_))
    }
}

impl Op {
    /// Try to convert a DataFusion expression to an Op. This function will return an error if the
    /// expression is not supported.
    pub fn try_from_expr(expr: &Expr, schema: SchemaRef) -> Result<Self, ParseExpressionError> {
        match expr {
            Expr::Column(column) => {
                let column_name = column.name();
                let column_type = schema
                    .field_with_name(column_name)
                    .map_err(|e| {
                        ParseExpressionError::column_not_found(
                            column_name.to_owned(),
                            schema.clone(),
                            e,
                        )
                    })?
                    .data_type()
                    .try_into()?;
                Ok(Op::col(column_name, column_type))
            }
            Expr::Literal(scalar_value) => {
                let value_type = ValueType::try_from(&scalar_value.data_type())?;
                let value = match value_type {
                    ValueType::Date => {
                        let days: u64 = scalar_value.to_string().parse().unwrap();
                        let value = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(Days::new(days))
                            .unwrap()
                            .format("%Y-%m-%d")
                            .to_string();
                        value
                    }
                    _ => scalar_value.to_string(),
                };
                Ok(Op::lit(value, value_type))
            }
            Expr::BinaryExpr(binary_expr) => {
                let left = Op::try_from_expr(&binary_expr.left, schema.clone())?;
                let right = Op::try_from_expr(&binary_expr.right, schema.clone())?;
                // if (!left.is_leaf() || !right.is_leaf()) &&  {
                //     println!("{:?} {:?}", left, right);
                //     return Err(ParseExpressionError::BinaryOperationDoesNotSupportNonLeaf);
                // }
                match binary_expr.op {
                    datafusion::logical_expr::Operator::Eq => Ok(Op::eq(left, right)),
                    datafusion::logical_expr::Operator::Lt => Ok(Op::less_than(left, right)),
                    datafusion::logical_expr::Operator::LtEq => {
                        Ok(Op::less_than_or_equal(left, right))
                    }
                    datafusion::logical_expr::Operator::Gt => Ok(Op::greater_than(left, right)),
                    datafusion::logical_expr::Operator::GtEq => {
                        Ok(Op::greater_than_or_equal(left, right))
                    }
                    datafusion::logical_expr::Operator::And => Ok(Op::and(vec![left, right])),
                    datafusion::logical_expr::Operator::Or => Ok(Op::or(vec![left, right])),
                    _ => Err(ParseExpressionError::unsupported_expression(
                        expr.variant_name().to_string(),
                    )),
                }
            }
            Expr::Not(expr) => {
                let child = Op::try_from_expr(expr, schema)?;
                Ok(Op::not(child))
            }
            Expr::IsNotNull(expr) => {
                let child = Op::try_from_expr(expr, schema)?;
                Ok(Op::not(Op::is_null(child)))
            }
            Expr::IsNull(expr) => {
                let child = Op::try_from_expr(expr, schema)?;
                if !child.is_column() {
                    return Err(ParseExpressionError::IsNullCanOnlyBeAppliedToColumns);
                }
                Ok(Op::is_null(child))
            }
            Expr::IsTrue(expr) => {
                let left = Op::try_from_expr(expr, schema)?;
                let right = Op::lit("true", ValueType::Bool);
                Ok(Op::eq(left, right))
            }
            Expr::IsFalse(expr) => {
                let left = Op::try_from_expr(expr, schema)?;
                let right = Op::lit("false", ValueType::Bool);
                Ok(Op::eq(left, right))
            }
            Expr::IsNotTrue(expr) => {
                let left = Op::try_from_expr(expr, schema)?;
                let right = Op::lit("true", ValueType::Bool);
                Ok(Op::not(Op::eq(left, right)))
            }
            Expr::IsNotFalse(expr) => {
                let left = Op::try_from_expr(expr, schema)?;
                let right = Op::lit("false", ValueType::Bool);
                Ok(Op::not(Op::eq(left, right)))
            }
            e => Err(ParseExpressionError::unsupported_expression(
                e.variant_name().to_owned(),
            )),
        }
    }

    fn from_expr(expr: &Expr, schema: SchemaRef) -> Result<Self, DeltaSharingError> {
        let converted = match expr {
            Expr::Column(col) => {
                let name = &col.name;
                let value_type = schema
                    .field_with_name(name)
                    .map_err(|e| DeltaSharingError::other(e.to_string()))?
                    .data_type()
                    .try_into()?;
                Op::col(name, value_type)
            }
            Expr::Literal(lit) => {
                let value_type = ValueType::try_from(&lit.data_type())?;
                match value_type {
                    ValueType::Date => match lit {
                        ScalarValue::Date32(Some(days)) => {
                            let value = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .checked_add_days(Days::new(*days as u64))
                                .unwrap()
                                .format("%Y-%m-%d")
                                .to_string();
                            Op::lit(value, value_type)
                        }
                        _ => {
                            panic!("invalid data_value")
                        }
                    },
                    _ => {
                        let value = lit.to_string();
                        Op::lit(value, value_type)
                    }
                }
            }
            Expr::BinaryExpr(bin) => {
                let left = Op::from_expr(&bin.left, schema.clone())?;
                let right = Op::from_expr(&bin.right, schema.clone())?;
                match bin.op {
                    datafusion::logical_expr::Operator::Eq => Op::eq(left, right),
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
            Expr::IsNotNull(child) => {
                let child = Op::from_expr(child, schema)?;
                Op::not(Op::is_null(child))
            }
            Expr::IsNull(child) => {
                let child = Op::from_expr(child, schema)?;
                Op::is_null(child)
            }
            _ => return Err(DeltaSharingError::other("Filter not supported")),
        };

        Ok(converted)
    }
}

impl serde::Serialize for Op {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Op::Column(column) => {
                let mut ser = serializer.serialize_struct("column", 3)?;
                ser.serialize_field("op", "column")?;
                ser.serialize_field("name", &column.name)?;
                ser.serialize_field("valueType", &column.value_type)?;
                ser.end()
            }
            Op::Literal(literal) => {
                let mut ser = serializer.serialize_struct("literal", 3)?;
                ser.serialize_field("op", "literal")?;
                ser.serialize_field("value", &literal.value)?;
                ser.serialize_field("valueType", &literal.value_type)?;
                ser.end()
            }
            Op::IsNull(op) => {
                let mut s = serializer.serialize_struct("isNull", 2)?;
                s.serialize_field("op", "isNull")?;
                s.serialize_field("children", &[op])?;
                s.end()
            }
            Op::Equal { left, right } => {
                let mut s = serializer.serialize_struct("equal", 2)?;
                s.serialize_field("op", "equal")?;
                s.serialize_field("children", &[left, right])?;
                s.end()
            }
            Op::LessThan { left, right } => {
                let mut s = serializer.serialize_struct("lessThan", 2)?;
                s.serialize_field("op", "lessThan")?;
                s.serialize_field("children", &[left, right])?;
                s.end()
            }
            Op::LessThanOrEqual { left, right } => {
                let mut s = serializer.serialize_struct("lessThanOrEqual", 2)?;
                s.serialize_field("op", "lessThanOrEqual")?;
                s.serialize_field("children", &[left, right])?;
                s.end()
            }
            Op::GreaterThan { left, right } => {
                let mut s = serializer.serialize_struct("greaterThan", 2)?;
                s.serialize_field("op", "greaterThan")?;
                s.serialize_field("children", &[left, right])?;
                s.end()
            }
            Op::GreaterThanOrEqual { left, right } => {
                let mut s = serializer.serialize_struct("greaterThanOrEqual", 2)?;
                s.serialize_field("op", "greaterThanOrEqual")?;
                s.serialize_field("children", &[left, right])?;
                s.end()
            }
            Op::And(ops) => {
                let mut s = serializer.serialize_struct("and", 2)?;
                s.serialize_field("op", "and")?;
                s.serialize_field("children", &ops)?;
                s.end()
            }
            Op::Or(ops) => {
                let mut s = serializer.serialize_struct("or", 2)?;
                s.serialize_field("op", "or")?;
                s.serialize_field("children", &ops)?;
                s.end()
            }
            Op::Not(op) => {
                let mut s = serializer.serialize_struct("not", 2)?;
                s.serialize_field("op", "not")?;
                s.serialize_field("children", &[op])?;
                s.end()
            }
        }
    }
}

impl BitAnd for Op {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Op::and(vec![self, rhs])
    }
}

impl BitOr for Op {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Op::or(vec![self, rhs])
    }
}

impl Not for Op {
    type Output = Self;

    fn not(self) -> Self::Output {
        Op::not(self)
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Column {
    name: String,
    value_type: ValueType,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Literal {
    value: String,
    value_type: ValueType,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
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
    type Error = ParseExpressionError;

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
            dt => {
                return Err(ParseExpressionError::unsupported_column_data_type(
                    dt.to_string(),
                ))
            }
        };
        Ok(converted)
    }
}

#[cfg(test)]
mod serialize_op {
    use super::*;

    #[test]
    fn serialize_column_op() {
        let op = Op::col("column_name", ValueType::String);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_literal_op() {
        let op = Op::lit("literal_value", ValueType::String);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_is_null_op() {
        let op = Op::is_null(Op::col("column_name", ValueType::String));
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_equal_op() {
        let left = Op::col("column_name", ValueType::String);
        let right = Op::lit("literal_value", ValueType::String);
        let op = Op::eq(left, right);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_less_than_op() {
        let left = Op::col("column_name", ValueType::Int);
        let right = Op::lit("25", ValueType::Int);
        let op = Op::less_than(left, right);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_less_than_or_equal_op() {
        let left = Op::col("column_name", ValueType::Int);
        let right = Op::lit("25", ValueType::Int);
        let op = Op::less_than_or_equal(left, right);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_greater_than_op() {
        let left = Op::col("column_name", ValueType::Int);
        let right = Op::lit("25", ValueType::Int);
        let op = Op::greater_than(left, right);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_greater_than_or_equal_op() {
        let left = Op::col("column_name", ValueType::Int);
        let right = Op::lit("25", ValueType::Int);
        let op = Op::greater_than_or_equal(left, right);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_and_op() {
        let sub_op1 = Op::eq(
            Op::col("a", ValueType::String),
            Op::lit("1", ValueType::String),
        );
        let sub_op2 = Op::eq(
            Op::col("b", ValueType::String),
            Op::lit("2", ValueType::String),
        );
        let sub_op3 = Op::eq(
            Op::col("c", ValueType::String),
            Op::lit("3", ValueType::String),
        );
        let op = Op::and(vec![sub_op1, sub_op2, sub_op3]);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_or_op() {
        let sub_op1 = Op::eq(
            Op::col("a", ValueType::String),
            Op::lit("1", ValueType::String),
        );
        let sub_op2 = Op::eq(
            Op::col("b", ValueType::String),
            Op::lit("2", ValueType::String),
        );
        let sub_op3 = Op::eq(
            Op::col("c", ValueType::String),
            Op::lit("3", ValueType::String),
        );
        let op = Op::or(vec![sub_op1, sub_op2, sub_op3]);
        insta::assert_json_snapshot!(op);
    }

    #[test]
    fn serialize_not_op() {
        let child = Op::eq(
            Op::col("a", ValueType::String),
            Op::lit("1", ValueType::String),
        );
        let op = Op::not(child);
        insta::assert_json_snapshot!(op);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::{Field, Schema};
    use datafusion::{logical_expr::Operator, prelude::*, scalar::ScalarValue};

    use super::*;

    #[test]
    fn equal_op_from_expr() {
        let column = col("`hireDate`");
        let literal = Expr::Literal(ScalarValue::Date32(Some(18746)));
        let expr = binary_expr(column, datafusion::logical_expr::Operator::Eq, literal);
        let schema = Schema::new(vec![Field::new("hireDate", DataType::Date32, false)]);

        let op = Op::from_expr(&expr, Arc::new(schema)).unwrap();
        insta::assert_json_snapshot!(op);
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
        insta::assert_json_snapshot!(parsed_op);
    }

    #[test]
    fn not_op_from_expr() {
        let column = col("`id`");
        let expr = Expr::Not(Box::new(Expr::IsNull(Box::new(column))));
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let parsed_op = Op::from_expr(&expr, Arc::new(schema)).unwrap();
        insta::assert_json_snapshot!(parsed_op);
    }

    #[test]
    fn flatten_and_ops() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]);
        let expr1 = binary_expr(col("a"), Operator::Eq, lit("1"));
        let expr2 = binary_expr(col("b"), Operator::Eq, lit("2"));
        let expr3 = binary_expr(col("c"), Operator::Eq, lit("3"));
        let expr = and(expr1, and(expr2, expr3));

        let op = Op::try_from_expr(&expr, Arc::new(schema)).unwrap();
        insta::assert_json_snapshot!(op);
    }
}
