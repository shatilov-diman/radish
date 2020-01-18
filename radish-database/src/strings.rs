/* Copyright (c) 2020 Dmitry Shatilov <shatilov dot diman at gmail dot com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::iter::FromIterator;
use std::time::{SystemTime, Duration};
use std::collections::VecDeque;

use indexmap::map::Entry;

use super::container::Container;
use super::container::ContainerPtr;
use super::container::ContainerImpl;

type Key = super::Key;
type Value = super::Value;
type Arguments = super::Arguments;
type ExecResult = super::ExecResult;

type Inner = Vec<u8>;

#[derive(Clone, Copy)]
enum BitOperation {
	And,
	Or,
	Xor,
	Not,
}

impl std::str::FromStr for BitOperation {
	type Err = String;

	fn from_str(op: &str) -> Result<Self, Self::Err> {
		match &op.to_lowercase()[..] {
			"and" => Ok(BitOperation::And),
			"or" =>  Ok(BitOperation::Or),
			"xor" => Ok(BitOperation::Xor),
			"not" => Ok(BitOperation::Not),
			op@_ => Err(format!("Unsupported operation '{}'", op)),
		}
	}
}

fn inner_parse<T>(cnt: &Inner, def: T) -> Result<T, String>
where	T: std::str::FromStr,
	<T as std::str::FromStr>::Err: std::fmt::Display
{
	if cnt.len() == 0 {
		Ok(def)
	} else {
		let str = std::str::from_utf8(cnt).map_err(|e|format!("{}", e))?;
		str.parse::<T>().map_err(|e|format!("{}", e))
	}
}


impl super::Storage {
	fn strings_container_factory() -> Container {
		Container::Strings(ContainerImpl::<Inner>::new())
	}
	async fn strings_get_container(&self, key: Key) -> ContainerPtr {
		self.get_container(key, Self::strings_container_factory).await
	}
	async fn strings_get_containers(&self, keys: Vec<Key>) -> Vec<ContainerPtr> {
		self.get_containers(keys, Self::strings_container_factory).await
	}
	async fn strings_try_get_containers(&self, keys: &Vec<Key>) -> Vec<Option<ContainerPtr>> {
		self.try_get_containers(keys).await
	}
	fn strings_unwrap_container(container: &Container) -> Result<&ContainerImpl<Inner>, String> {
		match container {
			Container::Strings(ref c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	fn strings_unwrap_mut_container(container: &mut Container) -> Result<&mut ContainerImpl<Inner>, String> {
		match container {
			Container::Strings(ref mut c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn strings_lock<F: FnOnce(&Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.strings_get_container(key).await;
		let c2 = c1.lock().await;
		let c3 = Self::strings_unwrap_container(&c2)?;
		processor(&c3.inner)
	}
	async fn strings_lock_mut<F: FnOnce(&mut Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.strings_get_container(key).await;
		let mut c2 = c1.lock().await;
		let c3 = Self::strings_unwrap_mut_container(&mut c2)?;
		processor(&mut c3.inner)
	}

	async fn strings_locks<F>(&self, write_keys: Vec<Key>, read_keys: &Vec<Key>, callback: F) -> ExecResult
	where F: FnOnce(VecDeque<&mut ContainerImpl<Inner>>, VecDeque<Option<&ContainerImpl<Inner>>>) -> ExecResult {
		let write_containers = self.strings_get_containers(write_keys).await;
		let read_containers = self.strings_try_get_containers(read_keys).await;
		let writes = write_containers.iter().map(|x|x.as_ref());
		let reads = read_containers.iter().map(|x|{
			match x {
				None => None,
				Some(x) => Some(x.as_ref()),
			}
		});
		let (mut writes, reads) = Self::lock_all(writes, reads).await;

		let mut out_writes = VecDeque::with_capacity(writes.len());
		for g in &mut writes {
			out_writes.push_back(Self::strings_unwrap_mut_container(&mut **g)?);
		}
		let mut out_reads = VecDeque::with_capacity(reads.len());
		for g in &reads {
			match g {
				None => out_reads.push_back(None),
				Some(g) => out_reads.push_back(Some(Self::strings_unwrap_container(&**g)?)),
			}
		}

		callback(out_writes, out_reads)
	}

	pub async fn strings_append(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;
		self.strings_lock_mut(key, |cnt| -> ExecResult {
			cnt.append(&mut value.into_iter().collect());
			Ok(Value::Integer(cnt.len() as i64))
		}).await
	}

	pub async fn strings_get(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.strings_locks(vec![], &vec![key], |_, mut cnts| -> ExecResult {
			let cnt = cnts.remove(0).expect("option should be exists, but not");
			match cnt {
				Some(cnt) => Ok(Value::Buffer(cnt.inner.clone())),
				None => Ok(Value::Nill),
			}
		}).await
	}

	pub async fn strings_set(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;

		let mut keepttl = false;
		let mut expire: Option<SystemTime> = None;
		let mut set_if_exists: Option<bool> = None;

		while let Some(subcmd) = Self::extract_string(args.pop_front()).ok() {
			match &subcmd.to_uppercase()[..] {
				"KEEPTTL" => keepttl = true,
				"XX" => set_if_exists = Some(true),
				"NX" => set_if_exists = Some(false),
				"EX" => expire = Some(SystemTime::now() + Duration::from_secs(Self::extract_unsigned_integer(args.pop_front())?)),
				"PX" => expire = Some(SystemTime::now() + Duration::from_millis(Self::extract_unsigned_integer(args.pop_front())?)),
				arg => return Err(format!("Unexpected argument '{}'", arg)),
			}
		}

		let mut cnt = ContainerImpl::<Inner>::new();
		cnt.inner = value;
		if ! keepttl {
			cnt.expiration_time = None;
		}
		if let Some(expire) = expire {
			cnt.expiration_time = Some(expire);
		}
		let cnt = Self::make_container(Container::Strings(cnt));

		let mut containers = self.containers.lock().await;
		let entry = containers.entry(key.clone());
		let result = match (set_if_exists, entry) {
			(None, Entry::Vacant(e)) | (Some(false), Entry::Vacant(e)) => {
				e.insert(cnt);
				Ok(Value::Ok)
			},
			(None, Entry::Occupied(mut e)) | (Some(true), Entry::Occupied(mut e)) => {
				*e.get_mut() = cnt;
				Ok(Value::Ok)
			},
			_ => Ok(Value::Nill),
		};
		drop(containers);

		if let (Ok(Value::Ok), Some(timepoint)) = (result.clone(), expire) {
			self.expire_key_at(&key, timepoint).await;
		}
		result
	}

	pub async fn strings_setex_impl(&mut self, key: Key, timepoint: SystemTime, value: Vec<u8>) -> ExecResult {
		let cnt = self.strings_get_container(key.clone()).await;
		let mut cnt = cnt.lock().await;
		let mut cnt = Self::strings_unwrap_mut_container(&mut cnt)?;

		cnt.inner = value;
		cnt.expiration_time = Some(timepoint);
		drop(cnt);

		self.expire_key_at(&key, timepoint).await;
		Ok(Value::Ok)
	}

	pub async fn strings_setex(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let seconds = Self::extract_unsigned_integer(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;
		let timepoint = SystemTime::now() + Duration::from_secs(seconds);
		self.strings_setex_impl(key, timepoint, value).await
	}

	pub async fn strings_psetex(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let millis = Self::extract_unsigned_integer(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;
		let timepoint = SystemTime::now() + Duration::from_millis(millis);
		self.strings_setex_impl(key, timepoint, value).await
	}

	pub async fn strings_setnx(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;

		let mut cnt = ContainerImpl::<Inner>::new();
		cnt.inner = value;
		cnt.expiration_time = None;
		let cnt = Self::make_container(Container::Strings(cnt));

		let mut containers = self.containers.lock().await;
		match containers.entry(key.clone()) {
			Entry::Occupied(_) => Ok(Value::Bool(false)),
			Entry::Vacant(e) => {
				e.insert(cnt);
				Ok(Value::Bool(true))
			},
		}
	}

	pub async fn strings_getset(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;
		let mut value: Inner = value.into();
		self.strings_locks(vec![key], &vec![], |mut cnt, _| {
			let mut cnt = cnt.remove(0).expect("key should be created, but not");
			cnt.expiration_time = None;
			std::mem::swap(&mut cnt.inner, &mut value);
			Ok(Value::Nill)
		}).await.unwrap();
		Ok(Value::Buffer(value.into()))
	}

	pub async fn strings_len(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.strings_lock(key, |cnt| -> ExecResult {
			Ok(Value::Integer(cnt.len() as i64))
		}).await
	}

	pub async fn strings_incrby(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = if let Ok(value) = Self::extract_integer(args.pop_front()) {value} else {1};
		self.strings_lock_mut(key, |cnt| -> ExecResult {
			let number = inner_parse::<i64>(cnt, 0)?;
			let number = number + value;
			*cnt = format!("{}", number).as_bytes().to_vec();
			Ok(Value::Integer(number))
		}).await
	}

	pub async fn strings_decrby(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = if let Ok(value) = Self::extract_integer(args.pop_front()) {value} else {1};
		self.strings_lock_mut(key, |cnt| -> ExecResult {
			let number = inner_parse::<i64>(cnt, 0)?;
			let number = number - value;
			*cnt = format!("{}", number).as_bytes().to_vec();
			Ok(Value::Integer(number))
		}).await
	}

	pub async fn strings_incrby_float(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let value = if let Ok(value) = Self::extract_integer(args.pop_front()) {value} else {1};
		self.strings_lock_mut(key, |cnt| -> ExecResult {
			let number = if cnt.len() == 0 {0} else {std::str::from_utf8(cnt).map_err(|e|format!("{}", e))?.parse::<i64>().map_err(|e|format!("{}", e))?};
			let number = number + value;
			*cnt = format!("{}", number).as_bytes().to_vec();
			Ok(Value::Integer(number))
		}).await
	}

	pub async fn strings_bitcount(&self, mut args: Arguments) -> ExecResult {
		static BITCOUNTMAP: [u8; 256] = [0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8];

		let key = Self::extract_key(args.pop_front())?;
		let start = if let Ok(start) = Self::extract_integer(args.pop_front()) {start} else {0};
		let end = if let Ok(end) = Self::extract_integer(args.pop_front()) {end} else {-1};
		self.strings_lock(key, |cnt| -> ExecResult {
			let start =     if start >= 0 {start} else {cnt.len() as i64 + start} as usize;
			let end   = 1 + if end   >= 0 {  end} else {cnt.len() as i64 +   end} as usize;
			if start >= cnt.len() || start >= end {
				return Ok(Value::Integer(0));
			}
			let sum: u64 = cnt
				.iter()
				.skip(start)
				.take(end - start)
				.map(|ch|BITCOUNTMAP[*ch as usize] as u64)
				.sum();
			Ok(Value::Integer(sum as i64))
		}).await
	}

	pub async fn strings_mget(&self, mut args: Arguments) -> ExecResult {
		let keys = args.drain(..).filter_map(|a|Self::extract_key(Some(a)).ok()).collect();
		self.strings_locks(vec![], &keys, |_, cnts| {
			let mut out = VecDeque::with_capacity(cnts.len());
			for cnt in cnts {
				match cnt {
					Some(cnt) => out.push_back(Value::Buffer(cnt.inner.clone())),
					None => out.push_back(Value::Nill),
				}
			}
			Ok(Value::Array(out))
		}).await
	}

	pub async fn strings_mset(&self, mut args: Arguments) -> ExecResult {
		let mut keys = Vec::with_capacity(args.len() / 2);
		let mut values = VecDeque::with_capacity(args.len() / 2);
		while args.len() > 1 {
			if let Ok(key) = Self::extract_key(args.pop_front()) {
				keys.push(key);
				let value = Self::extract_buffer(args.pop_front())?;
				values.push_back(value);
			}
		}
		self.strings_locks(keys, &vec![], |cnts, _| {
			for mut cnt in cnts {
				cnt.inner = values.pop_front().unwrap();
				cnt.expiration_time = None;
			}
			Ok(Value::Ok)
		}).await
	}

	pub async fn strings_bitop(&self, mut args: Arguments) -> ExecResult {
		match Self::extract_string(args.pop_front())?.parse::<BitOperation>()? {
			BitOperation::Not => self.strings_bitop_not(args).await,
			op => self.strings_bitop_cmn(op, args).await,
		}
	}

	async fn strings_bitop_not(&self, mut args: Arguments) -> ExecResult {
		let dest = Self::extract_key(args.pop_front())?;
		let src = Self::extract_key(args.pop_front())?;
		self.strings_locks(vec![dest], &vec![src], |mut dest, mut cnts| {
			let dest = dest.remove(0).ok_or("BITOP NOT dst src")?;
			let src = cnts.remove(0).ok_or("BITOP NOT dst src")?;

			dest.expiration_time = None;
			dest.inner = match src {
				Some(src) => Vec::from_iter(src.inner.iter().map(|ch|!*ch)),
				None => Vec::new(),
			};
			Ok(Value::Integer(dest.inner.len() as i64))
		}).await
	}

	async fn strings_bitop_cmn(&self, operation: BitOperation, mut args: Arguments) -> ExecResult {
		let dest = Self::extract_key(args.pop_front())?;
		let keys = args.drain(..).filter_map(|a|Self::extract_key(Some(a)).ok()).collect();

		self.strings_locks(vec![dest], &keys, |mut dest, mut cnts| {
			let max_len = cnts.iter()
				.map(|cnt|if cnt.is_none() {0} else {cnt.unwrap().inner.len()})
				.max().unwrap_or(0);
			let min_len = cnts.iter()
				.map(|cnt|if cnt.is_none() {0} else {cnt.unwrap().inner.len()})
				.min().unwrap_or(0);

			let unexpected_cnts_error = "BITOP <OPERATION> dst src [[src]]";
			let dest = dest.remove(0).ok_or(unexpected_cnts_error)?;
			let src = cnts.remove(0).ok_or(unexpected_cnts_error)?;

			dest.expiration_time = None;
			dest.inner = match src {
				Some(src) => src.inner.clone(),
				None => Inner::with_capacity(max_len),
			};
			dest.inner.resize(max_len, 0);

			match operation {
				BitOperation::And => if min_len > 0 {
					cnts.iter().filter_map(|cnt|cnt.as_ref())
					.for_each(|cnt| {
						for i in 0..min_len {
							match (dest.inner.get_mut(i), cnt.inner.get(i)) {
								(Some(d), Some(c)) => *d = *d & *c,
								_ => panic!("Unexpected arm"),
							}
						}
					});
				},
				op@BitOperation::Or | op@BitOperation::Xor => {
					cnts.iter().filter_map(|cnt|cnt.as_ref())
					.for_each(|cnt| {
						for i in 0..cnt.inner.len() {
							match (op, dest.inner.get_mut(i), cnt.inner.get(i)) {
								(BitOperation::Or,  Some(d), Some(c)) => *d = *d | *c,
								(BitOperation::Xor, Some(d), Some(c)) => *d = *d ^ *c,
								_ => panic!("Unexpected arm"),
							}
						}
					});
				},
				BitOperation::Not => panic!("Unexpected arm"),
			}
			Ok(Value::Integer(dest.inner.len() as i64))
		}).await
	}

	pub async fn strings_setbit(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let offset = Self::extract_integer(args.pop_front())? as usize;
		let bit = Self::extract_bit(args.pop_front())?;

		if offset >= 2^32 {
			return Err("offset is out of range [0; 2^32)".to_owned());
		}
		let byte_index = offset / 8;
		let bit_index = offset % 8;
		let mut mask = 0b1000_0000;
		mask >>= bit_index;

		self.strings_lock_mut(key, |cnt| -> ExecResult {
			if byte_index >= cnt.len() {
				cnt.resize(1 + byte_index, 0);
			}
			let byte = cnt.get_mut(byte_index).unwrap();
			let original = *byte & mask;
			if bit {
				*byte = *byte | mask;
			} else {
				*byte = *byte & !mask;
			}
			match original {
				0 => Ok(Value::Bool(false)),
				_ => Ok(Value::Bool(true)),
			}
		}).await
	}

	pub async fn strings_getbit(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let offset = Self::extract_integer(args.pop_front())? as usize;
		if offset >= 2^32 {
			return Err("offset is out of range [0; 2^32)".to_owned());
		}
		let byte_index = offset / 8;
		let bit_index = offset % 8;
		let mut mask = 0b1000_0000;
		mask >>= bit_index;

		self.strings_lock(key, |cnt| -> ExecResult {
			if byte_index >= cnt.len() {
				return Ok(Value::Bool(false));
			}
			let byte = cnt.get(byte_index).unwrap();
			let bit = *byte & mask;
			match bit {
				0 => Ok(Value::Bool(false)),
				_ => Ok(Value::Bool(true)),
			}
		}).await
	}

	pub async fn strings_get_range(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let start = Self::extract_integer(args.pop_front())?;
		let end = Self::extract_integer(args.pop_front())?;
		self.strings_lock(key, |cnt| -> ExecResult {
			let start =     if start >= 0 {start} else {cnt.len() as i64 + start} as usize;
			let end   = 1 + if end   >= 0 {  end} else {cnt.len() as i64 +   end} as usize;
			if start >= cnt.len() || start >= end {
				return Ok(Value::Buffer(vec![]));
			}
			let iter = cnt
				.iter()
				.skip(start)
				.take(end - start);
			Ok(Value::Buffer(Vec::from_iter(iter.cloned())))
		}).await
	}

	pub async fn strings_set_range(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let start = Self::extract_index(args.pop_front())?;
		let value = Self::extract_buffer(args.pop_front())?;
		let end = start + value.len();

		self.strings_lock_mut(key, |cnt| -> ExecResult {
			if cnt.len() < end {
				cnt.resize(end, 0);
			}
			cnt[start..end].copy_from_slice(&value[..]);
			Ok(Value::Integer(cnt.len() as i64))
		}).await
	}
}

