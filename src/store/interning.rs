#![cfg(feature = "interning")]

use string_interner::{backend::StringBackend, DefaultSymbol, StringInterner};
use tokio::sync::RwLock;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct TidStore
{
	interner: StringInterner<StringBackend<DefaultSymbol>>,
}

pub type SharedTidStore = Arc<RwLock<TidStore>>;

impl TidStore
{
	pub fn find(&self, name: &str) -> Option<DefaultSymbol>
	{
		self.interner.get(name.to_lowercase().trim())
	}

	pub fn intern(&mut self, name: &str) -> DefaultSymbol
	{
		self.interner.get_or_intern(name.to_lowercase().trim())
	}

	pub fn resolve(&self, sym: &DefaultSymbol) -> Option<&str>
	{
		self.interner.resolve(*sym)
	}
}
