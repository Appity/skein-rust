use std::convert::TryFrom;
use std::fmt::{self,Display};

use lapin::message::Delivery;
use serde::{Serialize, Deserialize};
use serde_json::json;
use serde_json::Value;
use serde::ser::{SerializeStruct,Serializer};
use serde::de::{self,Deserializer,Visitor,SeqAccess,MapAccess};

#[derive(Clone,Debug,Eq,PartialEq)]
pub struct Request {
    id : String,
    method : String,
    params : Option<Value>,
    reply_to : bool
}

impl Request {
    pub fn new(id: impl ToString, method: impl ToString, params: Option<Value>) -> Self {
        Self {
            id: id.to_string(),
            method: method.to_string(),
            params,
            reply_to: true
        }
    }

    pub fn new_serialize(id: impl ToString, method: impl ToString, params: Option<impl Into<Value>>) -> Self {
        Self {
            id: id.to_string(),
            method: method.to_string(),
            params: params.map(|p| p.into()),
            reply_to: true
        }
    }

    pub fn new_noreply(id: impl ToString, method: impl ToString, params: Option<Value>) -> Self {
        Self {
            id: id.to_string(),
            method: method.to_string(),
            params,
            reply_to: false
        }
    }

    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn method(&self) -> &String {
        &self.method
    }

    pub fn params(&self) -> Option<&Value> {
        self.params.as_ref()
    }

    pub fn reply_to(&self) -> bool {
        self.reply_to
    }
}

impl Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.params.is_some() {
            write!(f, "{}: {}({:?})", &self.id, &self.method, &self.params)
        }
        else {
            write!(f, "{}: {}", &self.id, &self.method)
        }
    }
}

impl Serialize for Request {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.params {
            Some(params) => json!({
                "jsonrpc": "2.0",
                "id": self.id,
                "method": &self.method,
                "params": params
            }),
            None => json!({
                "jsonrpc": "2.0",
                "id": self.id,
                "method": &self.method
            })
        }.serialize(serializer)
    }
}

impl TryFrom<&Delivery> for Request {
    type Error = Response;

    fn try_from(delivery: &Delivery) -> Result<Self, Response> {
        match std::str::from_utf8(&delivery.data) {
            Ok(s) => {
                match serde_json::from_str::<Value>(s) {
                    Ok(v) => {
                        match &v["jsonrpc"] {
                            Value::String(ver) => {
                                match ver.as_str() {
                                    "2.0" => {
                                        match serde_json::from_str::<Request>(s) {
                                            Ok(request) => Ok(request),
                                            Err(err) => {
                                                log::warn!("Error: JSON-RPC deserialization error {:?}", err);

                                                Err(
                                                    Response::new_error_without_id(
                                                        ErrorResponse::new(-32700, "Parse error, invalid JSON", None)
                                                    )
                                                )
                                            }
                                        }
                                    },
                                    ver => {
                                        log::warn!("Error: Mismatched JSON-RPC version {:?}", ver);

                                        Err(
                                            Response::new_error_without_id(
                                                ErrorResponse::new(-32600, "Invalid JSON-RPC version number", None)
                                            )
                                        )
                                    }
                                }
                            },
                            Value::Null => {
                                log::warn!("Error: \"jsonrpc\" attribute missing");

                                Err(
                                    Response::new_error_without_id(
                                        ErrorResponse::new(-32600, "Missing JSON-RPC version", None)
                                    )
                                )
                            },
                            _ => {
                                log::warn!("Error: \"jsonrpc\" attribute is not a string");

                                Err(
                                    Response::new_error_without_id(
                                        ErrorResponse::new(-32603, "Non-string JSON-RPC version field", None)
                                    )
                                )
                            }
                        }
                    },
                    Err(e) => {
                        log::warn!("Error: Invalid JSON in message ({})", e);

                        Err(
                            Response::new_error_without_id(
                                ErrorResponse::new(-32700, "Parse error, invalid JSON", None)
                            )
                        )
                    }
                }
            },
            Err(e) => {
                log::warn!("Error: Invalid UTF-8 in message ({})", e);

                Err(
                    Response::new_error_without_id(
                        ErrorResponse::new(-32603, "Internal processing error", None)
                    )
                )
            }
        }
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field { Version, Id, Method, Params }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`id`, `method` or `params`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "jsonrpc" => Ok(Field::Version),
                            "id" => Ok(Field::Id),
                            "method" => Ok(Field::Method),
                            "params" => Ok(Field::Params),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct RequestVisitor;

        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = Request;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Request")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Request, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let _version : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let id : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let method : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let params : Option<Value> = seq.next_element()?;

                Ok(Request::new(id, method, params))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Request, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut method = None;
                let mut params = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Version => {
                            // Value is ignored, but must be consumed.
                            let _version : String = map.next_value()?;
                        },
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }

                            id = Some(map.next_value()?);
                        }
                        Field::Method => {
                            if method.is_some() {
                                return Err(de::Error::duplicate_field("method"));
                            }

                            method = Some(map.next_value()?);
                        }
                        Field::Params => {
                            if params.is_some() {
                                return Err(de::Error::duplicate_field("params"));
                            }

                            params = Some(map.next_value()?);
                        }
                    }
                }

                let id : String = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let method : String = method.ok_or_else(|| de::Error::missing_field("method"))?;

                Ok(Request::new(id, method, params))
            }
        }

        const FIELDS: &[&str] = &[ "id", "request", "params" ];

        deserializer.deserialize_struct("Request", FIELDS, RequestVisitor)
    }
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum Response {
    Result {
        id: String,
        result: Value
    },
    Error {
        id: Option<String>,
        error: ErrorResponse
    }
}

impl Response {
    pub fn new_result(id: impl ToString, result: Value) -> Self {
        Self::Result {
            id: id.to_string(),
            result
        }
    }

    pub fn new_result_serialize(id: impl ToString, result: impl Into<Value>) -> Self {
        Self::Result {
            id: id.to_string(),
            result: result.into()
        }
    }

    pub fn new_error(id: impl ToString, error: ErrorResponse) -> Self {
        Self::Error {
            id: Some(id.to_string()),
            error
        }
    }

    pub fn new_error_without_id(error: ErrorResponse) -> Self {
        Self::Error {
            id: None,
            error
        }
    }

    pub fn result_for(request: &Request, result: Value) -> Self {
        Self::Result {
            id: request.id.clone(), result
        }
    }

    pub fn error_for(request: &Request, code: i32, message: impl ToString, data: Option<Value>) -> Self {
        Self::Error {
            id: Some(request.id.clone()),
            error: ErrorResponse::new(code, message, data)
        }
    }

    pub fn id(&self) -> Option<&String> {
        match self {
            Self::Result { id, .. } => Some(id),
            Self::Error { id, .. } => id.as_ref()
        }
    }

    pub fn is_result(&self) -> bool {
        matches!(self, Self::Result { .. })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    pub fn result(&self) -> Option<&Value> {
        if let Self::Result { result, .. } = &self {
            Some(result)
        }
        else {
            None
        }
    }
}

impl Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::Result { id, result } => write!(f, "{}: {:?}", id, result),
            Self::Error { id: None, error } => write!(f, "-: {:?}", error),
            Self::Error { id: Some(id), error } => write!(f, "{}: {:?}", id, error)
        }
    }
}

impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field { Version, Id, Result, Error }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`id` or `result`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "jsonrpc" => Ok(Field::Version),
                            "id" => Ok(Field::Id),
                            "result" => Ok(Field::Result),
                            "error" => Ok(Field::Error),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ResponseVisitor;

        impl<'de> Visitor<'de> for ResponseVisitor {
            type Value = Response;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Response")
            }

            // FUTURE: Figure out how to handle branching in a serializer.
            // fn visit_seq<V>(self, mut seq: V) -> Result<Response, V::Error>
            // where
            //     V: SeqAccess<'de>,
            // {
            //     let _version : String = seq.next_element()?
            //         .ok_or_else(|| de::Error::invalid_length(0, &self))?;
            //     let id : String = seq.next_element()?
            //         .ok_or_else(|| de::Error::invalid_length(1, &self))?;
            //     let result : Value = seq.next_element()?
            //         .ok_or_else(|| de::Error::invalid_length(2, &self))?;

            //     Ok(Response::new(id, result))
            // }

            fn visit_map<V>(self, mut map: V) -> Result<Response, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut result = None;
                let mut error = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Version => {
                            // Value is ignored, but must be consumed.
                            let _version : String = map.next_value()?;
                        },
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }

                            id = Some(map.next_value()?);
                        }
                        Field::Result => {
                            if result.is_some() {
                                return Err(de::Error::duplicate_field("result"));
                            }

                            result = Some(map.next_value()?);
                        }
                        Field::Error => {
                            if error.is_some() {
                                return Err(de::Error::duplicate_field("error"));
                            }

                            error = Some(map.next_value()?);
                        }
                    }
                }

                let id : String = id.ok_or_else(|| de::Error::missing_field("id"))?;

                if let Some(result) = result {
                    Ok(Response::new_result(id, result))
                }
                else if let Some(error) = error {
                    Ok(Response::new_error(id, error))
                }
                else {
                    Err(de::Error::missing_field("result"))
                }
            }
        }

        const FIELDS: &[&str] = &[ "jsonrpc", "id", "result", "error" ];

        deserializer.deserialize_struct("Response", FIELDS, ResponseVisitor)
    }
}

impl Serialize for Response {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Response", 3)?;

        state.serialize_field("jsonrpc", "2.0")?;

        match &self {
            Self::Result { id, result } => {
                state.serialize_field("id", id)?;
                state.serialize_field("result", result)?;
            },
            Self::Error { id, error } => {
                state.serialize_field("id", id)?;
                state.serialize_field("error", error)?;
            }
        }

        state.end()
    }
}

impl TryFrom<&Delivery> for Response {
    type Error = &'static str;

    fn try_from(delivery: &Delivery) -> Result<Self, &'static str> {
        match std::str::from_utf8(&delivery.data) {
            Ok(s) => {
                match serde_json::from_str::<Value>(s) {
                    Ok(v) => {
                        match &v["jsonrpc"] {
                            Value::String(ver) => {
                                match ver.as_str() {
                                    "2.0" => {
                                        match serde_json::from_str::<Response>(s) {
                                            Ok(request) => Ok(request),
                                            Err(err) => {
                                                log::warn!("Error: JSON-RPC deserialization error {:?}", err);

                                                Err("Parse error, invalid JSON")
                                            }
                                        }
                                    },
                                    ver => {
                                        log::warn!("Error: Mismatched JSON-RPC version {:?}", ver);

                                        Err("Invalid JSON-RPC version number")
                                    }
                                }
                            },
                            Value::Null => {
                                log::warn!("Error: \"jsonrpc\" attribute missing");

                                Err("Missing JSON-RPC version")
                            },
                            _ => {
                                log::warn!("Error: \"jsonrpc\" attribute is not a string");

                                Err("Non-string JSON-RPC version field")
                            }
                        }
                    },
                    Err(e) => {
                        log::warn!("Error: Invalid JSON in message ({})", e);

                        Err("Parse error, invalid JSON")
                    }
                }
            },
            Err(e) => {
                log::warn!("Error: Invalid UTF-8 in message ({})", e);

                Err("Internal processing error")
            }
        }
    }
}

#[derive(Clone,Debug,Eq,PartialEq,Deserialize,Serialize)]
pub struct ErrorResponse {
    code : i32,
    message : String,
    #[serde(skip_serializing_if="Option::is_none")]
    data : Option<Value>
}

impl ErrorResponse {
    pub fn new(code: i32, message: impl ToString, data: Option<Value>) -> Self {
        Self {
            code,
            message: message.to_string(),
            data
        }
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn message(&self) -> &String {
        &self.message
    }

    pub fn data(&self) -> Option<&Value> {
        self.data.as_ref()
    }
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} ({})", &self.message, &self.code)
    }
}

impl serde::ser::StdError for ErrorResponse {
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_request_new() {
        let request = Request::new("0ff0", "echo", None);

        assert_eq!(request.id, "0ff0");
        assert_eq!(request.method, "echo");
        assert_eq!(request.params, None);
    }

    #[test]
    fn test_request_serialize_no_params() {
        let request = Request::new("0ff0", "echo", None);

        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff0\",\"method\":\"echo\"}"
        );
    }

    #[test]
    fn test_request_serialize_with_params() {
        let request = Request::new("0ff0", "echo", Some(json!({ "example": true })));

        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff0\",\"method\":\"echo\",\"params\":{\"example\":true}}"
        );
    }

    #[test]
    fn test_request_deserialize_no_params() {
        let request : Request = serde_json::from_str(
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff0\",\"method\":\"echo\"}"
        ).unwrap();

        assert_eq!(request.id(), "0ff0");
        assert_eq!(request.method(), "echo");
        assert_eq!(request.params(), None);
    }

    #[test]
    fn test_request_deserialize_with_params() {
        let request : Request = serde_json::from_str(
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff0\",\"method\":\"echo\",\"params\":{\"example\":true}}"
        ).unwrap();

        assert_eq!(request.id(), "0ff0");
        assert_eq!(request.method(), "echo");
        assert_eq!(request.params(), Some(&json!({ "example": true })));
    }

    #[test]
    fn test_response_new() {
        let response = Response::new_result("0ff1", json!("echoed"));

        assert_eq!(response.id(), Some(&"0ff1".to_string()));
        assert_eq!(response.result(), Some(&json!("echoed")));
    }

    #[test]
    fn test_response_serialize() {
        let response = Response::new_result("0ff1", json!({ "nested": {"structure": true } }));

        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff1\",\"result\":{\"nested\":{\"structure\":true}}}"
        );
    }

    #[test]
    fn test_response_deserialize() {
        let response : Response = serde_json::from_str(
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff1\",\"result\":{\"nested\":{\"structure\":true}}}"
        ).unwrap();

        assert_eq!(response.id(), Some(&"0ff1".to_string()));
        assert_eq!(response.result(), Some(&json!({ "nested": {"structure": true } })));
    }

    #[test]
    fn test_error_serialize() {
        let error = ErrorResponse::new(-32000, "Test message", None);

        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            "{\"code\":-32000,\"message\":\"Test message\"}"
        );
    }

    #[test]
    fn test_error_deserialize() {
        let error : ErrorResponse = serde_json::from_str(
            "{\"code\":-32000,\"message\":\"Test message\"}"
        ).unwrap();


        assert_eq!(error.code, -32000);
        assert_eq!(error.message, "Test message");
        assert_eq!(error.data, None);
    }
}
