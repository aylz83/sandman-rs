use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use tokio::io::{SeekFrom, AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use pufferfish::prelude::*;

use crate::bed::{FieldValue, ParseContext, BedKind, BedFields, BedRecord, AnyBedRecord, IntoAnyBedRecord};
use crate::store::TidResolver;

use crate::error;

compile_error!("This feature (bigbed) is unstable, not fully implemented, and not yet supported.");

#[async_trait::async_trait]
impl<Resolver, Tid> BedFields<Resolver, Tid> for BigBedExtra
where
	Resolver: TidResolver<Tid = Tid> + Debug + Clone + Send + Sync + 'static,
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::BigBed;

	async fn parse_into<'a, 'b>(
		resolver: Arc<Mutex<Resolver>>,
		input: &'a [u8],
		ctx: Option<ParseContext<'b>>,
		record: &mut BedRecord<Resolver, Tid, Self>,
	) -> error::Result<&'a [u8]>
	{
		if let Some(ParseContext::BigBed(header)) = ctx
		{
			let mut cursor = input;

			let endian = header.endian;
			let schema = header.schema.as_deref();
			let tid_lookup: &Vec<String> = header.tid_lookup.as_ref();

			let tid_num = read_u32_from_bytes(&mut cursor, endian)?;
			let start = read_u64_from_bytes(&mut cursor, endian)?;
			let end = read_u64_from_bytes(&mut cursor, endian)?;

			let tid_id = tid_lookup
				.get(tid_num as usize)
				.ok_or(error::Error::TidNotFound("".to_string()))?;

			let tid_id = resolver.lock().await.to_symbol_id(&tid_id);

			match schema
			{
				Some(schema) =>
				{
					let mut fields = Vec::with_capacity(schema.len());
					for (name, field_type) in schema
					{
						let value = parse_field_from_bytes(&mut cursor, field_type, header.endian)?;
						fields.push((name.clone(), value));
					}

					let extra = BigBedExtra {
						fields: Some(fields),
					};

					*record =
						BedRecord::new_with_extra(Arc::clone(&resolver), tid_id, start, end, extra);
				}
				None =>
				{
					let extra = BigBedExtra { fields: None };
					*record =
						BedRecord::new_with_extra(Arc::clone(&resolver), tid_id, start, end, extra);
				}
			}

			Ok(cursor)
		}
		else
		{
			return Err(error::Error::NotBigBed);
		}
	}

	async fn empty(resolver: Arc<Mutex<Resolver>>) -> BedRecord<Resolver, Tid, Self>
	{
		let mut r = resolver.lock().await;
		BedRecord {
			resolver: resolver.clone(),
			tid: r.dummy_tid(),
			start: 0,
			end: 0,
			fields: BigBedExtra::default(),
		}
	}
}

async fn read_endian<R: AsyncRead + Unpin>(reader: &mut R) -> error::Result<Endian>
{
	let magic = reader.read_u32_le().await?;

	if magic == BIGBED_MAGIC
	{
		Ok(Endian::Little)
	}
	else if magic.swap_bytes() == BIGBED_MAGIC
	{
		Ok(Endian::Big)
	}
	else
	{
		Err(error::Error::NotBigBed)
	}
}

fn parse_field_from_bytes(
	input: &mut &[u8],
	field_type: &FieldType,
	endian: Endian,
) -> error::Result<FieldValue>
{
	match field_type
	{
		FieldType::Int => Ok(FieldValue::Int(read_i32_from_bytes(input, endian)?)),
		FieldType::UInt => Ok(FieldValue::UInt(read_u32_from_bytes(input, endian)?)),
		FieldType::Float => Ok(FieldValue::Float(read_f32_from_bytes(input, endian)?)),
		FieldType::Double => Ok(FieldValue::Double(read_f64_from_bytes(input, endian)?)),
		FieldType::String => Ok(FieldValue::String(read_string_from_bytes(input)?)),
		FieldType::Char { len } => Ok(FieldValue::Char(read_fixed_char_from_bytes(input, *len)?)),
		FieldType::IntArray => Ok(FieldValue::IntArray(read_int_array_from_bytes(
			input, endian,
		)?)),
		FieldType::UIntArray => Ok(FieldValue::UIntArray(read_uint_array_from_bytes(
			input, endian,
		)?)),
		FieldType::FloatArray => Ok(FieldValue::FloatArray(read_float_array_from_bytes(
			input, endian,
		)?)),
		FieldType::StringArray => Ok(FieldValue::StringArray(read_string_array_from_bytes(
			input,
		)?)),
	}
}

#[inline]
pub fn read_u32_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<u32>
{
	if input.len() < 4
	{
		return Err(error::Error::NotBigBed);
	}
	let (head, tail) = input.split_at(4);
	*input = tail;
	Ok(match endian
	{
		Endian::Little => u32::from_le_bytes(head.try_into().map_err(|_| error::Error::NotBigBed)?),
		Endian::Big => u32::from_be_bytes(head.try_into().map_err(|_| error::Error::NotBigBed)?),
	})
}

#[inline]
pub fn read_i32_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<i32>
{
	if input.len() < 4
	{
		return Err(error::Error::NotBigBed);
	}
	let (head, tail) = input.split_at(4);
	*input = tail;
	Ok(match endian
	{
		Endian::Little => i32::from_le_bytes(head.try_into().map_err(|_| error::Error::NotBigBed)?),
		Endian::Big => i32::from_be_bytes(head.try_into().map_err(|_| error::Error::NotBigBed)?),
	})
}

#[inline]
pub fn read_u64_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<u64>
{
	if input.len() < 8
	{
		return Err(error::Error::NotBigBed);
	}
	let (head, tail) = input.split_at(8);
	*input = tail;
	Ok(match endian
	{
		Endian::Little => u64::from_le_bytes(head.try_into().map_err(|_| error::Error::NotBigBed)?),
		Endian::Big => u64::from_be_bytes(head.try_into().map_err(|_| error::Error::NotBigBed)?),
	})
}

#[inline]
pub fn read_f32_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<f32>
{
	let bits = read_u32_from_bytes(input, endian)?;
	Ok(f32::from_bits(bits))
}

#[inline]
pub fn read_f64_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<f64>
{
	let bits = read_u64_from_bytes(input, endian)?;
	Ok(f64::from_bits(bits))
}

#[inline]
pub fn read_string_from_bytes(input: &mut &[u8]) -> error::Result<String>
{
	// null-terminated string
	if let Some(pos) = input.iter().position(|&b| b == 0)
	{
		let (head, tail) = input.split_at(pos);
		*input = &tail[1..]; // skip null byte
		Ok(String::from_utf8(head.to_vec()).map_err(|_| error::Error::NotBigBed)?)
	}
	else
	{
		Err(error::Error::NotBigBed)
	}
}

#[inline]
pub fn read_fixed_char_from_bytes(input: &mut &[u8], len: usize) -> error::Result<String>
{
	if input.len() < len
	{
		return Err(error::Error::NotBigBed);
	}
	let (head, tail) = input.split_at(len);
	*input = tail;
	let s = head
		.iter()
		.take_while(|&&b| b != 0)
		.cloned()
		.collect::<Vec<_>>();
	Ok(String::from_utf8(s).map_err(|_| error::Error::NotBigBed)?)
}

#[inline]
pub fn read_int_array_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<Vec<i32>>
{
	let len = read_u32_from_bytes(input, endian)? as usize;
	let mut vec = Vec::with_capacity(len);
	for _ in 0..len
	{
		vec.push(read_i32_from_bytes(input, endian)?);
	}
	Ok(vec)
}

#[inline]
pub fn read_uint_array_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<Vec<u32>>
{
	let len = read_u32_from_bytes(input, endian)? as usize;
	let mut vec = Vec::with_capacity(len);
	for _ in 0..len
	{
		vec.push(read_u32_from_bytes(input, endian)?);
	}
	Ok(vec)
}

#[inline]
pub fn read_float_array_from_bytes(input: &mut &[u8], endian: Endian) -> error::Result<Vec<f32>>
{
	let len = read_u32_from_bytes(input, endian)? as usize;
	let mut vec = Vec::with_capacity(len);
	for _ in 0..len
	{
		vec.push(read_f32_from_bytes(input, endian)?);
	}
	Ok(vec)
}

#[inline]
pub fn read_string_array_from_bytes(input: &mut &[u8]) -> error::Result<Vec<String>>
{
	let len = read_u32_from_bytes(input, Endian::Little)? as usize; // length prefix is always little?
	let mut vec = Vec::with_capacity(len);
	for _ in 0..len
	{
		vec.push(read_string_from_bytes(input)?);
	}
	Ok(vec)
}

#[derive(Debug, Clone)]
pub enum FieldType
{
	Int,
	UInt,
	Float,
	Double,
	String,
	Char
	{
		len: usize,
	},
	UIntArray,
	IntArray,
	FloatArray,
	StringArray,
}

#[derive(Debug, Clone)]
pub struct BigBedIndex
{
	pub endian: Endian,
	pub version: u16,
	pub zoom_levels: u16,
	pub tid_tree_offset: u64,
	pub data_offset: u64,
	pub rtree_offset: u64,
	pub field_count: u16,
	pub defined_field_count: u16,
	pub auto_sql_offset: u64,
	pub total_summary_offset: u64,
	pub uncompress_buf_size: u32,

	pub schema: Option<Vec<(String, FieldType)>>,
	pub tid_table: BTreeMap<String, (u32, u64)>,
	pub tid_lookup: Vec<String>,
}

impl BigBedIndex
{
	pub async fn offsets_for_tid<R>(
		&self,
		reader: &mut R,
		tid: &str,
	) -> error::Result<Vec<(u64, u32)>>
	where
		R: AsyncRead + AsyncSeekExt + Unpin,
	{
		let (_tid_num, tid_size) = self
			.tid_table
			.get(tid)
			.ok_or(error::Error::TidNotFound(tid.to_string()))?;

		self.offsets_for_tid_region(reader, tid, 0, *tid_size).await
	}

	// returns (file_offset, compressed_size)
	pub async fn offsets_for_tid_region<R>(
		&self,
		_reader: &mut R,
		_tid: &str,
		_start: u64,
		_end: u64,
	) -> error::Result<Vec<(u64, u32)>>
	where
		R: AsyncRead + AsyncSeekExt + Unpin,
	{
		todo!()
	}

	pub async fn read_index<R>(reader: &mut R) -> error::Result<BigBedIndex>
	where
		R: AsyncRead + AsyncSeekExt + Unpin,
	{
		println!("read_index");
		let endian = read_endian(reader).await?;

		println!("endian = {:?}", endian);

		let version = read_u16(reader, endian).await?;
		println!("version = {:?}", version);
		let zoom_levels = read_u16(reader, endian).await?;
		println!("zoom_levels = {:?}", zoom_levels);
		let tid_tree_offset = read_u64(reader, endian).await?;
		println!("tid_tree_offset = {:?}", tid_tree_offset);
		let data_offset = read_u64(reader, endian).await?;
		println!("data_offset = {:?}", data_offset);
		let rtree_offset = read_u64(reader, endian).await?;
		println!("rtree_offset = {:?}", rtree_offset);
		let field_count = read_u16(reader, endian).await?;
		println!("field_count = {:?}", field_count);
		let defined_field_count = read_u16(reader, endian).await?;
		println!("defined_field_count = {:?}", defined_field_count);
		let auto_sql_offset = read_u64(reader, endian).await?;
		println!("auto_sql_offset = {:?}", auto_sql_offset);
		let total_summary_offset = read_u64(reader, endian).await?;
		println!("total_summary_offset = {:?}", total_summary_offset);
		let uncompress_buf_size = read_u32(reader, endian).await?;
		println!("uncompress_buf_size = {:?}", uncompress_buf_size);
		let _reserved = read_u32(reader, endian).await?;
		println!("_reserved = {:?}", _reserved);

		let tid_table = Self::read_bptree(reader, endian, tid_tree_offset).await?;
		println!("tid_table = {:?}", tid_table);

		let schema = Self::read_autosql(reader, auto_sql_offset).await?;

		let mut tid_lookup = Vec::new();
		for (tid, (tid_num, _size)) in &tid_table
		{
			let tid_num = *tid_num as usize;
			if tid_lookup.len() <= tid_num
			{
				tid_lookup.resize(tid_num + 1, String::new());
			}
			tid_lookup[tid_num] = tid.clone();
		}

		let header = BigBedIndex {
			endian,
			version,
			zoom_levels,
			tid_tree_offset,
			data_offset,
			rtree_offset,
			field_count,
			defined_field_count,
			auto_sql_offset,
			total_summary_offset,
			uncompress_buf_size,
			schema,
			tid_table,
			tid_lookup,
		};

		println!("index = {:?}", header);
		Ok(header)
	}

	pub fn parse_autosql(input: &str) -> error::Result<Vec<(String, FieldType)>>
	{
		let mut fields = Vec::new();
		let mut in_block = false;

		for line in input.lines()
		{
			let line = line.trim();

			if line.starts_with('(')
			{
				in_block = true;
				continue;
			}
			if line.starts_with(')')
			{
				break;
			}
			if !in_block || line.is_empty()
			{
				continue;
			}

			// Remove trailing comments
			let line = line.split('"').next().unwrap_or("").trim();
			let line = line.trim_end_matches(';');

			let mut parts = line.split_whitespace();
			let ty = parts.next().ok_or(error::Error::MissingAutoSQLType)?;
			let name = parts.next().ok_or(error::Error::MissingAutoSQLField)?;

			let field_type = match ty
			{
				"int" => FieldType::Int,
				"uint" => FieldType::UInt,
				"float" => FieldType::Float,
				"double" => FieldType::Double,
				"string" => FieldType::String,
				t if t.starts_with("char[") =>
				{
					let len_str = t.trim_start_matches("char[").trim_end_matches(']');
					let len = len_str
						.parse::<usize>()
						.map_err(|_| error::Error::InvalidCharLength(len_str.to_string()))?;
					FieldType::Char { len }
				}
				"int[]" => FieldType::IntArray,
				"uint[]" => FieldType::UIntArray,
				"float[]" => FieldType::FloatArray,
				"string[]" => FieldType::StringArray,
				_ => continue, // safely skip unknown types
			};

			fields.push((name.to_string(), field_type));
		}

		Ok(fields)
	}

	async fn read_autosql<R>(
		reader: &mut R,
		offset: u64,
	) -> error::Result<Option<Vec<(String, FieldType)>>>
	where
		R: AsyncRead + AsyncSeekExt + Unpin,
	{
		if offset == 0
		{
			return Ok(None);
		}

		reader.seek(SeekFrom::Start(offset)).await?;

		let mut buf = Vec::new();
		loop
		{
			let b = reader.read_u8().await?;
			if b == 0
			{
				break;
			}
			buf.push(b);
		}

		let text = String::from_utf8(buf).map_err(|_| error::Error::NotBigBed)?;
		Ok(Some(Self::parse_autosql(&text)?)) // now returns names + types
	}

	async fn read_bptree<R>(
		reader: &mut R,
		endian: Endian,
		offset: u64,
	) -> error::Result<BTreeMap<String, (u32, u64)>>
	where
		R: AsyncRead + AsyncSeekExt + Unpin,
	{
		reader.seek(SeekFrom::Start(offset)).await?;

		let _magic = read_u32(reader, endian).await?;
		println!("_magic = {:?}", _magic);
		let _block_size = read_u32(reader, endian).await?;
		println!("_block_size = {:?}", _block_size);
		let key_size = read_u32(reader, endian).await?;
		println!("key_size = {:?}", key_size);
		let val_size = read_u32(reader, endian).await?;
		println!("val_size = {:?}", val_size);
		let item_count = read_u64(reader, endian).await?;
		println!("_item_count = {:?}", item_count);
		let _reserved = read_u64(reader, endian).await?;
		println!("_reserved = {:?}", _reserved);

		if val_size != 8
		{
			return Err(error::Error::NotBigBed);
		}

		let mut tid_table = BTreeMap::new();

		loop
		{
			let is_leaf = reader.read_u8().await?;
			let count = read_u16(reader, endian).await?;
			let _reserved = reader.read_u8().await?;

			println!("is_leaf = {}", is_leaf);
			println!("count = {}", count);
			println!("_reserved = {}", _reserved);

			if is_leaf == 1
			{
				for _ in 0..item_count
				{
					let mut name_buf = vec![0u8; key_size as usize];
					if reader.read_exact(&mut name_buf).await.is_err()
					{
						break; // EOF or corruption
					}

					println!("name_buf = {:?}", &name_buf);
					let tid = String::from_utf8_lossy(&name_buf)
						.trim_end_matches('\0')
						.to_string();

					let mut val_buf = vec![0u8; val_size as usize];
					reader.read_exact(&mut val_buf).await?;

					let tid_num = u32::from_le_bytes(val_buf[0..4].try_into().unwrap());
					let size = u32::from_le_bytes(val_buf[4..8].try_into().unwrap());

					println!("chrom = {:?}, tid = {} size = {}", tid, tid_num, size);
					tid_table.insert(tid, (tid_num, size as u64));
				}
				break;
			}
			else
			{
				// skip internal nodes
				let skip = 8 + (key_size as u64 + 8) * count as u64;
				reader.seek(SeekFrom::Current(skip as i64)).await?;
			}
		}

		Ok(tid_table)
	}
}

#[derive(Debug, Clone, Default)]
pub struct BigBedExtra
{
	pub fields: Option<Vec<(String, FieldValue)>>,
}

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, BigBedExtra>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::BigBed(self)
	}
}
