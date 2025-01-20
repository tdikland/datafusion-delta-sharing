//! Authentication errors

/// Errors that can occur when working with the DeltaSharing profile.
pub enum ProfileError {
    /// .
    ProfileFileDoesNotExist,
    /// perr
    ProfileFileParseError,
    /// Bearer
    BearerTokenExpired,
}
