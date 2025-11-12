use crate::bed::extra::Bed4Extra;
use crate::store::TidResolver;

use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Bed3Fields;

#[derive(Clone)]
pub struct BedRecord<Resolver, Tid, F>
where
	Resolver: TidResolver,
	Tid: Debug + Clone,
{
	pub(crate) resolver: Arc<Mutex<Resolver>>,
	pub tid: Tid,
	pub start: u32,
	pub end: u32,
	pub fields: F,
}

impl<Resolver, Tid, F> fmt::Debug for BedRecord<Resolver, Tid, F>
where
	Resolver: TidResolver,
	Tid: Debug + Clone,
	F: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
	{
		f.debug_struct("BedRecord")
			.field("tid", &self.tid)
			.field("start", &self.start)
			.field("end", &self.end)
			.field("fields", &self.fields)
			.finish()
	}
}

impl<Resolver, Tid> BedRecord<Resolver, Tid, Bed3Fields>
where
	Resolver: TidResolver,
	Tid: Debug + Clone,
{
	pub fn new(
		resolver: Arc<Mutex<Resolver>>,
		tid: Tid,
		start: u32,
		end: u32,
	) -> BedRecord<Resolver, Tid, Bed3Fields>
	{
		Self {
			resolver,
			tid: tid,
			start,
			end,
			fields: Bed3Fields,
		}
	}

	pub fn with_name(self, name: impl Into<String>) -> BedRecord<Resolver, Tid, Bed4Extra>
	{
		BedRecord {
			resolver: self.resolver,
			tid: self.tid,
			start: self.start,
			end: self.end,
			fields: Bed4Extra { name: name.into() },
		}
	}
}
