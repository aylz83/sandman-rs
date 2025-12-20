#![cfg(feature = "interning")]

use string_interner::{backend::StringBackend, DefaultSymbol, StringInterner};

#[derive(Clone, Debug, Default)]
pub struct TidStore
{
	interner: StringInterner<StringBackend<DefaultSymbol>>,
}

impl TidStore
{
	pub fn find(&self, name: &str) -> Option<DefaultSymbol>
	{
		self.interner.get(name.trim())
	}

	pub fn intern(&mut self, name: &str) -> DefaultSymbol
	{
		self.interner.get_or_intern(name.trim())
	}

	pub fn resolve(&self, sym: &DefaultSymbol) -> Option<&str>
	{
		self.interner.resolve(*sym)
	}
}
