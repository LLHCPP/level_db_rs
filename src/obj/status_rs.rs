use std::fmt;

// Define the error codes as an enum
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StatusCode {
    Ok = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IOError = 5,
}

// Custom error type
#[derive(Debug, Clone)]
pub struct Status {
    code: StatusCode,
    message: Option<String>,
}

impl Status {
    // Create a success status
    pub fn ok() -> Self {
        Status {
            code: StatusCode::Ok,
            message: None,
        }
    }

    // Error status constructors
    pub fn not_found(msg: &str, msg2: Option<&str>) -> Self {
        Self::new(StatusCode::NotFound, msg, msg2)
    }

    pub fn corruption(msg: &str, msg2: Option<&str>) -> Self {
        Self::new(StatusCode::Corruption, msg, msg2)
    }

    pub fn not_supported(msg: &str, msg2: Option<&str>) -> Self {
        Self::new(StatusCode::NotSupported, msg, msg2)
    }

    pub fn invalid_argument(msg: &str, msg2: Option<&str>) -> Self {
        Self::new(StatusCode::InvalidArgument, msg, msg2)
    }

    pub fn io_error(msg: &str, msg2: Option<&str>) -> Self {
        Self::new(StatusCode::IOError, msg, msg2)
    }

    // Private constructor
    fn new(code: StatusCode, msg: &str, msg2: Option<&str>) -> Self {
        let message = match msg2 {
            Some(m2) => format!("{}: {}", msg, m2),
            None => msg.to_string(),
        };
        Status {
            code,
            message: Some(message),
        }
    }

    // Status checking methods
    pub fn is_ok(&self) -> bool {
        self.code == StatusCode::Ok
    }

    pub fn is_not_found(&self) -> bool {
        self.code == StatusCode::NotFound
    }

    pub fn is_corruption(&self) -> bool {
        self.code == StatusCode::Corruption
    }

    pub fn is_io_error(&self) -> bool {
        self.code == StatusCode::IOError
    }

    pub fn is_not_supported_error(&self) -> bool {
        self.code == StatusCode::NotSupported
    }

    pub fn is_invalid_argument(&self) -> bool {
        self.code == StatusCode::InvalidArgument
    }

    pub fn code(&self) -> StatusCode {
        self.code
    }

    pub fn from_io_error(err: std::io::Error, filename: &str) -> Self {
        use std::io::ErrorKind;
        match err.kind() {
            ErrorKind::NotFound => Status::not_found(filename, Some(err.to_string().as_str())),
            _ => Status::io_error(filename, Some(err.to_string().as_str())),
        }
    }
}

// Implement Display for string representation
impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ok() {
            write!(f, "OK")
        } else {
            match &self.message {
                Some(msg) => write!(f, "{:?}: {}", self.code, msg),
                None => write!(f, "{:?}", self.code),
            }
        }
    }
}

// Implement std::error::Error trait
impl std::error::Error for Status {}

// Implement From trait for easy conversion from standard errors
impl From<std::io::Error> for Status {
    fn from(err: std::io::Error) -> Self {
        Status::io_error(&err.to_string(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_move_constructor() {
        let status = Status::ok();
        let status2 = status;
        assert!(status2.is_ok());
        let status = Status::not_found("custom NotFound status message", None);
        let status2 = status;
        assert!(status2.is_not_found());
        assert_eq!(
            "NotFound: custom NotFound status message",
            status2.to_string()
        );
    }
}
