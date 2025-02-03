use serde::{ser::SerializeMap, Serialize, Serializer};

pub struct Predicate {
    op: Box<dyn Op>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

struct ColumnOp {
    name: String,
    value_type: ValueType,
}

impl Serialize for ColumnOp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("op", "column")?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("valueType", &self.value_type)?;
        map.end()
    }
}

impl Op for ColumnOp {
    fn to_string(&self) -> String {
        serde_json::to_string(self).expect("valid")
    }
}

impl LeafOp for ColumnOp {}

struct LiteralOp {
    value: String,
    value_type: ValueType,
}

struct IsNullOp {
    column: ColumnOp,
}

struct EqualOp<L, R> {
    left: L,
    right: R,
}

struct LessThanOp<L, R> {
    left: L,
    right: R,
}

#[derive(Serialize)]
struct AndOp {
    ops: Vec<Box<dyn Op>>,
}

trait LeafOp: Op {}

trait Op {
    fn to_string(&self) -> String;
}
