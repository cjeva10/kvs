use crate::Result;

/// A state machine has an interior `Cmd` type that defines the type of commands that it receives
/// and implemennts `apply` which simply applies the given command to the machine
pub trait StateMachine<C: TryFrom<String>> {
    fn apply(&self, command: &C) -> Result<()>;
}

/// `DummyStateMachine` simply takes `String` commands and prints them to stdout.
pub struct DummyStateMachine {}

impl StateMachine<String> for DummyStateMachine {
    fn apply(&self, command: &String) -> Result<()> {
        println!("DummyStateMachine applying {}", command);

        Ok(())
    }
}
