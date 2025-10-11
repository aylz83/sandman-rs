use crate::bed::extra::Bed4Extra;

#[derive(Debug, Clone)]
pub struct Bed3Fields;

#[derive(Debug, Clone)]
pub struct BedRecord<F>
{
	pub tid: String,
	pub start: u32,
	pub end: u32,
	pub fields: F,
}

impl BedRecord<Bed3Fields>
{
	pub fn new(tid: impl Into<String>, start: u32, end: u32) -> Self
	{
		Self {
			tid: tid.into(),
			start,
			end,
			fields: Bed3Fields,
		}
	}

	pub fn with_name(self, name: impl Into<String>) -> BedRecord<Bed4Extra>
	{
		BedRecord {
			tid: self.tid,
			start: self.start,
			end: self.end,
			fields: Bed4Extra { name: name.into() },
		}
	}
}
