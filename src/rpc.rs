use std::fmt::{self,Display};

use serde_json::json;
use serde_json::Value;
use serde::ser::{Serialize,SerializeStruct,Serializer};
use serde::de::{self,Deserialize,Deserializer,Visitor,SeqAccess,MapAccess};

#[derive(Clone,Debug,Eq,PartialEq)]
pub struct Request {
    pub id : String,
    pub method : String,
    pub params : Option<Value>
}

impl Request {
    pub fn new(id: impl ToString, method: impl ToString, params: Option<Value>) -> Self {
        Self {
            id: id.to_string(),
            method: method.to_string(),
            params
        }
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

        const FIELDS: &'static [&'static str] = &[ "id", "request", "params" ];

        deserializer.deserialize_struct("Request", FIELDS, RequestVisitor)
    }
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub struct Response {
    pub id : String,
    pub result : Value
}

impl Response {
    pub fn new(id: impl ToString, result: Value) -> Self {
        Self {
            id: id.to_string(),
            result
        }
    }

    pub fn to(request: &Request, result: Value) -> Self {
        Self {
            id: request.id.clone(),
            result
        }
    }
}

impl Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", &self.id, &self.result)
    }
}

impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field { Version, Id, Result }

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

            fn visit_seq<V>(self, mut seq: V) -> Result<Response, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let _version : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let id : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let result : Value = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;

                Ok(Response::new(id, result))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Response, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut result = None;

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
                    }
                }

                let id : String = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let result : Value = result.ok_or_else(|| de::Error::missing_field("response"))?;

                Ok(Response::new(id, result))
            }
        }

        const FIELDS: &'static [&'static str] = &[ "id", "response" ];

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
        state.serialize_field("id", &self.id)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub struct Error {
    pub id : String,
    pub code : i32,
    pub message : String,
    pub data : Option<Value>
}

impl Error {
    pub fn new(id: impl ToString, code: i32, message: impl ToString, data: Option<Value>) -> Self {
        Self {
            id: id.to_string(),
            code,
            message: message.to_string(),
            data
        }
    }
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let error = match &self.data {
            Some(data) => json!({
                "code": self.code,
                "message": &self.message,
                "data": data
            }),
            None => json!({
                "code": self.code,
                "message": &self.message
            })
        };

        json!({
            "jsonrpc": "2.0",
            "id": self.id,
            "error": error
        }).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Error {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field { Version, Id, Code, Message, Data }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`id`, `code`, `message` or `data`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "jsonrpc" => Ok(Field::Version),
                            "id" => Ok(Field::Id),
                            "code" => Ok(Field::Code),
                            "message" => Ok(Field::Message),
                            "data" => Ok(Field::Data),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ErrorVisitor;

        impl<'de> Visitor<'de> for ErrorVisitor {
            type Value = Error;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Request")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Error, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let _version : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let id : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let code : i32 = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let message : String = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let data : Option<Value> = seq.next_element()?;

                Ok(Error::new(id, code, message, data))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Error, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut code = None;
                let mut message = None;
                let mut data = None;

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
                        Field::Code => {
                            if code.is_some() {
                                return Err(de::Error::duplicate_field("code"));
                            }

                            code = Some(map.next_value()?);
                        }
                        Field::Message => {
                            if message.is_some() {
                                return Err(de::Error::duplicate_field("message"));
                            }

                            message = Some(map.next_value()?);
                        }
                        Field::Data => {
                            if data.is_some() {
                                return Err(de::Error::duplicate_field("data"));
                            }

                            data = Some(map.next_value()?);
                        }
                    }
                }

                let id : String = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let code : i32 = code.ok_or_else(|| de::Error::missing_field("code"))?;
                let message : String = message.ok_or_else(|| de::Error::missing_field("method"))?;

                Ok(Error::new(id, code, message, data))
            }
        }

        const FIELDS: &'static [&'static str] = &[ "id", "message", "data" ];

        deserializer.deserialize_struct("Error", FIELDS, ErrorVisitor)
    }
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

        assert_eq!(request.id, "0ff0");
        assert_eq!(request.method, "echo");
        assert_eq!(request.params, None);
    }

    #[test]
    fn test_request_deserialize_with_params() {
        let request : Request = serde_json::from_str(
            "{\"jsonrpc\":\"2.0\",\"id\":\"0ff0\",\"method\":\"echo\",\"params\":{\"example\":true}}"
        ).unwrap();

        assert_eq!(request.id, "0ff0");
        assert_eq!(request.method, "echo");
        assert_eq!(request.params, Some(json!({ "example": true })));
    }

    #[test]
    fn test_response_new() {
        let response = Response::new("0ff1", json!("echoed"));

        assert_eq!(response.id, "0ff1");
        assert_eq!(response.result, json!("echoed"));
    }

    #[test]
    fn test_response_serialize() {
        let response = Response::new("0ff1", json!({ "nested": {"structure": true } }));

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

        assert_eq!(response.id, "0ff1");
        assert_eq!(response.result, json!({ "nested": {"structure": true } }));
    }

    #[test]
    fn test_error_serialize() {
        let error = Error::new("ff0f", -32000, "Test message", None);

        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            "{\"jsonrpc\":\"2.0\",\"id\":\"ff0f\",\"error\":{\"code\":-32000,\"message\":\"Test message\"}}"
        );
    }
}
