#[derive(Debug, PartialEq)]
pub enum FairOSUserError {
    Error,
    UsernameAlreadyExists,
    InvalidUsername,
    InvalidPassword,
}

#[derive(Debug, PartialEq)]
pub enum FairOSPodError {
    Error,
}

#[derive(Debug, PartialEq)]
pub enum FairOSError {
    CouldNotConnect,
    User(FairOSUserError),
    Pod(FairOSPodError),
}
