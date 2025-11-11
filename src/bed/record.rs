use crate::bed::extra::Bed4Extra;

use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct Bed3Fields;

#[derive(Debug, Clone)]
pub struct BedRecord<Tid, F>
where
	Tid: Debug + Clone,
{
	pub tid: Tid,
	pub start: u32,
	pub end: u32,
	pub fields: F,
}

impl<Tid> BedRecord<Tid, Bed3Fields>
where
	Tid: Debug + Clone,
{
	pub fn new(tid: Tid, start: u32, end: u32) -> Self
	{
		Self {
			tid: tid,
			start,
			end,
			fields: Bed3Fields,
		}
	}

	pub fn with_name(self, name: impl Into<String>) -> BedRecord<Tid, Bed4Extra>
	{
		BedRecord {
			tid: self.tid,
			start: self.start,
			end: self.end,
			fields: Bed4Extra { name: name.into() },
		}
	}
}
