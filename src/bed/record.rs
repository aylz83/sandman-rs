use crate::store::TidResolver;
use crate::bed::IntoAnyBedRecord;
use crate::bed::AnyBedRecord;

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
	pub start: u64,
	pub end: u64,
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

impl<Resolver, Tid, F> BedRecord<Resolver, Tid, F>
where
	Resolver: TidResolver<Tid = Tid>,
	Tid: Debug + Clone,
{
	pub async fn pretty_tid(&mut self) -> Option<String>
	{
		let mut r = self.resolver.lock().await;
		r.from_symbol_id(&self.tid).map(|s| s.to_string())
	}

	pub fn new_with_extra(
		resolver: Arc<Mutex<Resolver>>,
		tid: Tid,
		start: u64,
		end: u64,
		extra: F,
	) -> BedRecord<Resolver, Tid, F>
	{
		Self {
			resolver,
			tid: tid,
			start,
			end,
			fields: extra,
		}
	}
}

impl<Resolver, Tid> BedRecord<Resolver, Tid, Bed3Fields>
where
	Resolver: TidResolver<Tid = Tid>,
	Tid: Debug + Clone,
{
	pub fn new(
		resolver: Arc<Mutex<Resolver>>,
		tid: Tid,
		start: u64,
		end: u64,
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
}

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, Bed3Fields>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::Bed3(self)
	}
}
