pub enum Tokens {
    Select,
    Distinct,
    All,
}

pub trait ToTokens {
    fn to_tokens(&self) -> Vec<Tokens>;
}

// impl ToTokens for ResultCol
