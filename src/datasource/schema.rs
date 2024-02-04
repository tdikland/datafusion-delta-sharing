//! Delta table schema

use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Display};

const MAP_ROOT_DEFAULT: &str = "entries";
const MAP_KEY_DEFAULT: &str = "keys";
const MAP_VALUE_DEFAULT: &str = "values";
const LIST_ROOT_DEFAULT: &str = "item";

/// Represents a struct field defined in the Delta table schema.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StructField {
    /// Name of this (possibly nested) column
    pub name: String,
    /// The data type of this field
    #[serde(rename = "type")]
    pub data_type: DataType,
    /// Denotes whether this Field can be null
    pub nullable: bool,
    /// A JSON map containing information about this column
    pub metadata: HashMap<String, String>,
}

impl StructField {
    /// Creates a new field
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    /// Creates a new field with metadata
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.metadata = metadata
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    #[inline]
    /// Returns the name of the column
    pub fn name(&self) -> &String {
        &self.name
    }

    #[inline]
    /// Returns whether the column is nullable
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    /// Returns the data type of the column
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    /// Returns the metadata of the column
    pub const fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

/// A struct is used to represent both the top-level schema of the table
/// as well as struct columns that contain nested columns.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StructType {
    #[serde(rename = "type")]
    /// The type of this struct
    pub type_name: String,
    /// The type of element stored in this array
    pub fields: Vec<StructField>,
}

impl StructType {
    /// Creates a new struct type
    pub fn new(fields: Vec<StructField>) -> Self {
        Self {
            type_name: "struct".into(),
            fields,
        }
    }

    /// Returns an immutable reference of the fields in the struct
    pub fn fields(&self) -> &Vec<StructField> {
        &self.fields
    }
}

impl FromIterator<StructField> for StructType {
    fn from_iter<T: IntoIterator<Item = StructField>>(iter: T) -> Self {
        Self {
            type_name: "struct".into(),
            fields: iter.into_iter().collect(),
        }
    }
}

impl<'a> FromIterator<&'a StructField> for StructType {
    fn from_iter<T: IntoIterator<Item = &'a StructField>>(iter: T) -> Self {
        Self {
            type_name: "struct".into(),
            fields: iter.into_iter().cloned().collect(),
        }
    }
}

impl<const N: usize> From<[StructField; N]> for StructType {
    fn from(value: [StructField; N]) -> Self {
        Self {
            type_name: "struct".into(),
            fields: value.to_vec(),
        }
    }
}

impl<'a, const N: usize> From<[&'a StructField; N]> for StructType {
    fn from(value: [&'a StructField; N]) -> Self {
        Self {
            type_name: "struct".into(),
            fields: value.into_iter().cloned().collect(),
        }
    }
}

impl<'a> IntoIterator for &'a StructType {
    type Item = &'a StructField;
    type IntoIter = std::slice::Iter<'a, StructField>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.iter()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
/// An array stores a variable length collection of items of some type.
pub struct ArrayType {
    #[serde(rename = "type")]
    /// The type of this struct
    pub type_name: String,
    /// The type of element stored in this array
    pub element_type: DataType,
    /// Denoting whether this array can contain one or more null values
    pub contains_null: bool,
}

impl ArrayType {
    /// Creates a new array type
    pub fn new(element_type: DataType, contains_null: bool) -> Self {
        Self {
            type_name: "array".into(),
            element_type,
            contains_null,
        }
    }

    #[inline]
    /// Returns the element type of the array
    pub const fn element_type(&self) -> &DataType {
        &self.element_type
    }

    #[inline]
    /// Returns whether the array can contain null values
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
/// A map stores an arbitrary length collection of key-value pairs
pub struct MapType {
    #[serde(rename = "type")]
    /// The type of this struct
    pub type_name: String,
    /// The type of element used for the key of this map
    pub key_type: DataType,
    /// The type of element used for the value of this map
    pub value_type: DataType,
    /// Denoting whether this array can contain one or more null values
    #[serde(default = "default_true")]
    pub value_contains_null: bool,
}

impl MapType {
    /// Creates a new map type
    pub fn new(key_type: DataType, value_type: DataType, value_contains_null: bool) -> Self {
        Self {
            type_name: "map".into(),
            key_type,
            value_type,
            value_contains_null,
        }
    }

    #[inline]
    /// Returns the key type of the map
    pub const fn key_type(&self) -> &DataType {
        &self.key_type
    }

    #[inline]
    /// Returns the value type of the map
    pub const fn value_type(&self) -> &DataType {
        &self.value_type
    }

    #[inline]
    /// Returns whether the map can contain null values
    pub const fn value_contains_null(&self) -> bool {
        self.value_contains_null
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
/// Primitive types supported by Delta
pub enum PrimitiveType {
    /// UTF-8 encoded string of characters
    String,
    /// i64: 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807
    Long,
    /// i32: 4-byte signed integer. Range: -2147483648 to 2147483647
    Integer,
    /// i16: 2-byte signed integer numbers. Range: -32768 to 32767
    Short,
    /// i8: 1-byte signed integer number. Range: -128 to 127
    Byte,
    /// f32: 4-byte single-precision floating-point numbers
    Float,
    /// f64: 8-byte double-precision floating-point numbers
    Double,
    /// bool: boolean values
    Boolean,
    /// Binary: uninterpreted binary data
    Binary,
    /// Date: Calendar date (year, month, day)
    Date,
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp,
    // TODO: timestamp without timezone
    #[serde(
        serialize_with = "serialize_decimal",
        deserialize_with = "deserialize_decimal",
        untagged
    )]
    /// Decimal: arbitrary precision decimal numbers
    Decimal(u8, i8),
}

fn serialize_decimal<S: serde::Serializer>(
    precision: &u8,
    scale: &i8,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("decimal({},{})", precision, scale))
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<(u8, i8), D::Error>
where
    D: serde::Deserializer<'de>,
{
    let str_value = String::deserialize(deserializer)?;
    if !str_value.starts_with("decimal(") || !str_value.ends_with(')') {
        return Err(serde::de::Error::custom(format!(
            "Invalid decimal: {}",
            str_value
        )));
    }

    let mut parts = str_value[8..str_value.len() - 1].split(',');
    let precision = parts
        .next()
        .and_then(|part| part.trim().parse::<u8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid precision in decimal: {}", str_value))
        })?;
    let scale = parts
        .next()
        .and_then(|part| part.trim().parse::<i8>().ok())
        .ok_or_else(|| {
            serde::de::Error::custom(format!("Invalid scale in decimal: {}", str_value))
        })?;

    Ok((precision, scale))
}

impl Display for PrimitiveType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Integer => write!(f, "integer"),
            PrimitiveType::Short => write!(f, "short"),
            PrimitiveType::Byte => write!(f, "byte"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Binary => write!(f, "binary"),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Decimal(precision, scale) => {
                write!(f, "decimal({},{})", precision, scale)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged, rename_all = "camelCase")]
/// Top level delta tdatatypes
pub enum DataType {
    /// UTF-8 encoded string of characters
    Primitive(PrimitiveType),
    /// An array stores a variable length collection of items of some type.
    Array(Box<ArrayType>),
    /// A struct is used to represent both the top-level schema of the table as well
    /// as struct columns that contain nested columns.
    Struct(Box<StructType>),
    /// A map stores an arbitrary length collection of key-value pairs
    /// with a single keyType and a single valueType
    Map(Box<MapType>),
}

impl From<MapType> for DataType {
    fn from(map_type: MapType) -> Self {
        DataType::Map(Box::new(map_type))
    }
}

impl From<StructType> for DataType {
    fn from(struct_type: StructType) -> Self {
        DataType::Struct(Box::new(struct_type))
    }
}

impl From<ArrayType> for DataType {
    fn from(array_type: ArrayType) -> Self {
        DataType::Array(Box::new(array_type))
    }
}

#[allow(missing_docs)]
impl DataType {
    pub const STRING: Self = DataType::Primitive(PrimitiveType::String);
    pub const LONG: Self = DataType::Primitive(PrimitiveType::Long);
    pub const INTEGER: Self = DataType::Primitive(PrimitiveType::Integer);
    pub const SHORT: Self = DataType::Primitive(PrimitiveType::Short);
    pub const BYTE: Self = DataType::Primitive(PrimitiveType::Byte);
    pub const FLOAT: Self = DataType::Primitive(PrimitiveType::Float);
    pub const DOUBLE: Self = DataType::Primitive(PrimitiveType::Double);
    pub const BOOLEAN: Self = DataType::Primitive(PrimitiveType::Boolean);
    pub const BINARY: Self = DataType::Primitive(PrimitiveType::Binary);
    pub const DATE: Self = DataType::Primitive(PrimitiveType::Date);
    pub const TIMESTAMP: Self = DataType::Primitive(PrimitiveType::Timestamp);

    pub fn decimal(precision: u8, scale: i8) -> Self {
        DataType::Primitive(PrimitiveType::Decimal(precision, scale))
    }

    pub fn struct_type(fields: Vec<StructField>) -> Self {
        DataType::Struct(Box::new(StructType::new(fields)))
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Primitive(p) => write!(f, "{}", p),
            DataType::Array(a) => write!(f, "array<{}>", a.element_type),
            DataType::Struct(s) => {
                write!(f, "struct<")?;
                for (i, field) in s.fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Map(m) => write!(f, "map<{}, {}>", m.key_type, m.value_type),
        }
    }
}

impl TryFrom<&StructType> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(s: &StructType) -> Result<Self, ArrowError> {
        let fields = s
            .fields()
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<ArrowField>, ArrowError>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<&StructField> for ArrowField {
    type Error = ArrowError;

    fn try_from(f: &StructField) -> Result<Self, ArrowError> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| Ok((key.clone(), serde_json::to_string(val)?)))
            .collect::<Result<_, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;

        let field = ArrowField::new(
            f.name(),
            ArrowDataType::try_from(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFrom<&ArrayType> for ArrowField {
    type Error = ArrowError;
    fn try_from(a: &ArrayType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            LIST_ROOT_DEFAULT,
            ArrowDataType::try_from(a.element_type())?,
            // TODO check how to handle nullability
            a.contains_null(),
        ))
    }
}

impl TryFrom<&MapType> for ArrowField {
    type Error = ArrowError;

    fn try_from(a: &MapType) -> Result<Self, ArrowError> {
        Ok(ArrowField::new(
            MAP_ROOT_DEFAULT,
            ArrowDataType::Struct(
                vec![
                    ArrowField::new(
                        MAP_KEY_DEFAULT,
                        ArrowDataType::try_from(a.key_type())?,
                        false,
                    ),
                    ArrowField::new(
                        MAP_VALUE_DEFAULT,
                        ArrowDataType::try_from(a.value_type())?,
                        a.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            // always non-null
            false,
        ))
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(t: &DataType) -> Result<Self, ArrowError> {
        match t {
            DataType::Primitive(p) => {
                match p {
                    PrimitiveType::String => Ok(ArrowDataType::Utf8),
                    PrimitiveType::Long => Ok(ArrowDataType::Int64), // undocumented type
                    PrimitiveType::Integer => Ok(ArrowDataType::Int32),
                    PrimitiveType::Short => Ok(ArrowDataType::Int16),
                    PrimitiveType::Byte => Ok(ArrowDataType::Int8),
                    PrimitiveType::Float => Ok(ArrowDataType::Float32),
                    PrimitiveType::Double => Ok(ArrowDataType::Float64),
                    PrimitiveType::Boolean => Ok(ArrowDataType::Boolean),
                    PrimitiveType::Binary => Ok(ArrowDataType::Binary),
                    PrimitiveType::Decimal(precision, scale) => {
                        if precision <= &38 {
                            Ok(ArrowDataType::Decimal128(*precision, *scale))
                        } else if precision <= &76 {
                            Ok(ArrowDataType::Decimal256(*precision, *scale))
                        } else {
                            Err(ArrowError::SchemaError(format!(
                                "Precision too large to be represented in Arrow: {}",
                                precision
                            )))
                        }
                    }
                    PrimitiveType::Date => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone. Stored as 4 bytes integer representing days since 1970-01-01
                        Ok(ArrowDataType::Date32)
                    }
                    PrimitiveType::Timestamp => {
                        // Issue: https://github.com/delta-io/delta/issues/643
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                }
            }
            DataType::Struct(s) => Ok(ArrowDataType::Struct(
                s.fields()
                    .iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(Arc::new(m.as_ref().try_into()?), false)),
        }
    }
}
