use serde::Deserialize;

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Block(pub u64);

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Block {
    pub fn next(&self) -> Block {
        Block(self.0 + 1)
    }

    pub fn key(&self) -> String {
        format!("block_{}", self.0)
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct State(pub u64);

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl State {
    pub fn next(&self) -> State {
        State(self.0 + 1)
    }

    pub fn key(&self) -> String {
        format!("state_{}", self.0)
    }
}

#[derive(PartialEq, Eq, Deserialize)]
pub struct Class(pub String);

impl std::fmt::Display for Class {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Class {
    pub fn key(&self) -> String {
        format!("class_{}", self.0)
    }
}
