pub mod interning;

use std::fmt::Debug;

#[cfg(feature = "interning")]
pub use crate::store::interning::*;

#[cfg(feature = "interning")]
pub type DefaultTid = string_interner::DefaultSymbol;

#[cfg(not(feature = "interning"))]
pub type DefaultTid = String;

pub trait TidResolver
{
	type Tid: Clone + Debug + Send + Sync + PartialEq + Eq + Ord + PartialOrd;

	fn find(&self, input: &str) -> Option<Self::Tid>;
	fn to_symbol_id(&mut self, input: &str) -> Self::Tid;
	fn from_symbol_id<'a>(&'a mut self, input: &'a Self::Tid) -> Option<&'a str>;
	fn dummy_tid(&mut self) -> Self::Tid;
}

#[cfg(not(feature = "interning"))]
impl TidResolver for ()
{
	type Tid = String;

	fn find(&self, input: &str) -> Option<Self::Tid>
	{
		Some(input.to_owned())
	}

	fn to_symbol_id(&mut self, input: &str) -> Self::Tid
	{
		input.to_owned()
	}

	fn from_symbol_id<'a>(&'a mut self, input: &'a Self::Tid) -> Option<&'a str>
	{
		Some(input)
	}

	fn dummy_tid(&mut self) -> Self::Tid
	{
		String::new()
	}
}

#[cfg(feature = "interning")]
impl TidResolver for TidStore
{
	type Tid = string_interner::DefaultSymbol;

	fn find(&self, input: &str) -> Option<Self::Tid>
	{
		self.find(input)
	}

	fn to_symbol_id(&mut self, input: &str) -> Self::Tid
	{
		self.intern(input)
	}

	fn from_symbol_id<'a>(&'a mut self, input: &'a Self::Tid) -> Option<&'a str>
	{
		self.resolve(input)
	}

	fn dummy_tid(&mut self) -> Self::Tid
	{
		self.to_symbol_id("__DUMMY_TID__")
	}
}
