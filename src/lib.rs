pub mod bed;
pub mod error;
pub mod store;
pub mod tabix;

pub mod prelude
{
	pub use crate::bed::AnyBedRecord;
	pub use crate::bed::AutoBedRecord;
	pub use crate::bed::{Bed3Fields, Bed4Extra, Bed5Extra, Bed6Extra, Bed12Extra, BedMethylExtra};

	#[cfg(feature = "bigbed")]
	pub use crate::bed::bigbedrecord::BigBedExtra;
}
