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

use std::collections::VecDeque;

use indexmap::IndexMap;

use super::container::Container;
use super::container::ContainerPtr;
use super::container::ContainerImpl;

type Key = super::Key;
type Value = super::Value;
type Arguments = super::Arguments;
type ExecResult = super::ExecResult;

type Inner = IndexMap<Value, Value>;

impl super::Storage {
	async fn hash_get_container(&self, key: Key) -> ContainerPtr {
		self.get_container(key, ||Container::Hash(ContainerImpl::<Inner>::new())).await
	}
	async fn _hash_try_get_container(&self, key: &Key) -> Option<ContainerPtr> {
		self.try_get_container(key).await
	}
	async fn hash_unwrap_container(container: &Container) -> Result<&ContainerImpl<Inner>, String> {
		match container {
			Container::Hash(ref c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn hash_unwrap_mut_container(container: &mut Container) -> Result<&mut ContainerImpl<Inner>, String> {
		match container {
			Container::Hash(ref mut c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn hash_lock<F: FnOnce(&Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.hash_get_container(key).await;
		let c2 = c1.lock().await;
		let c3 = Self::hash_unwrap_container(&c2).await?;
		processor(&c3.inner)
	}
	async fn hash_lock_mut<F: FnOnce(&mut Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.hash_get_container(key).await;
		let mut c2 = c1.lock().await;
		let c3 = Self::hash_unwrap_mut_container(&mut c2).await?;
		processor(&mut c3.inner)
	}
	async fn _hash_try_lock_mut<F: FnOnce(&mut Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		match self._hash_try_get_container(&key).await {
			None => Ok(Value::Nill),
			Some(c1) => {
				let mut c2 = c1.lock().await;
				let c3 = Self::hash_unwrap_mut_container(&mut c2).await?;
				processor(&mut c3.inner)
			}
		}
	}

	pub async fn hash_set(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock_mut(key, |hash| -> ExecResult {
			let mut count = 0;
			while args.len() >= 2 {
				let field = args.pop_front().unwrap();
				let value = args.pop_front().unwrap();
				hash.insert(field, value);
				count = count + 1;
			}
			Ok(Value::Integer(count as i64))
		}).await
	}

	pub async fn hash_set_nx(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock_mut(key, |hash| -> ExecResult {
			let field = args.pop_front().unwrap();
			let value = args.pop_front().unwrap();
			if let indexmap::map::Entry::Vacant(place) = hash.entry(field) {
				place.insert(value);
				Ok(Value::Bool(true))
			} else {
				Ok(Value::Bool(false))
			}
		}).await
	}

	pub async fn hash_del(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock_mut(key, |hash| -> ExecResult {
			let mut count = 0;
			for field in args {
				if let Some(_) = hash.remove(&field) {
					count = count + 1;
				}
			}
			Ok(Value::Integer(count as i64))
		}).await
	}

	pub async fn hash_get(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let field = Self::extract(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			match hash.get(&field) {
				None => Ok(Value::Nill),
				Some(value) => Ok(value.clone()),
			}
		}).await
	}

	pub async fn hash_mget(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			let mut out = VecDeque::with_capacity(args.len());
			while let Some(field) = args.pop_front() {
			match hash.get(&field) {
				None => out.push_back(Value::Nill),
				Some(value) => out.push_back(value.clone()),
			}
			}
			Ok(Value::Array(out))
		}).await
	}

	pub async fn hash_get_all(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			let mut out = VecDeque::with_capacity(2 * hash.len());
			for (field, value) in hash {
				out.push_back(field.clone());
				out.push_back(value.clone());
			}
			Ok(Value::Array(out))
		}).await
	}

	pub async fn hash_exists(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let field = Self::extract(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			Ok(Value::Bool(hash.contains_key(&field)))
		}).await
	}

	pub async fn hash_keys(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			Ok(Value::Array(hash.keys().cloned().collect()))
		}).await
	}

	pub async fn hash_values(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			Ok(Value::Array(hash.values().cloned().collect()))
		}).await
	}

	pub async fn hash_len(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			Ok(Value::Integer(hash.len() as i64))
		}).await
	}

	pub async fn hash_strlen(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let field = Self::extract(args.pop_front())?;
		self.hash_lock(key, |hash| -> ExecResult {
			match hash.get(&field) {
				Some(Value::Buffer(value)) => Ok(Value::Integer(value.len() as i64)),
				_ => Ok(Value::Nill),
			}
		}).await
	}

	pub async fn hash_incrby(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let field = Self::extract(args.pop_front())?;
		let value = Self::extract_integer(args.pop_front())?;
		self.hash_lock_mut(key, |hash| -> ExecResult {
			match hash.entry(field).or_insert(Value::Integer(0)) {
				Value::Integer(v) => {
					*v = *v + value;
					Ok(Value::Integer(*v))
				},
				_ => Err(format!("Unexpected field type")),
			}
		}).await
	}

	pub async fn hash_incrbyfloat(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let field = Self::extract(args.pop_front())?;
		let value = Self::extract_float(args.pop_front())?;
		self.hash_lock_mut(key, |hash| -> ExecResult {
			match hash.entry(field).or_insert(Value::Float(0f64.to_bits())) {
				Value::Float(ref mut n) => {
					*n = (f64::from_bits(*n) + value).to_bits();
					Ok(Value::Float(*n))
				},
				_ => Err(format!("Unexpected field type")),
			}
		}).await
	}

	pub async fn hash_scan(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let start = Self::extract_index(args.pop_front())?;

		let mut pattern: Option<String> = None;
		let mut max_check = 100usize;

		while let Some(subcmd) = Self::extract_string(args.pop_front()).ok() {
			match &subcmd.to_uppercase()[..] {
				"MATCH" => pattern = Some(Self::extract_string(args.pop_front())?),
				"COUNT" => max_check = Self::extract_index(args.pop_front())?,
				arg => return Err(format!("Unexpected argument '{}'", arg)),
			}
		}

		let pattern = match pattern {
			None => None,
			Some(pattern) => Some(regex::bytes::Regex::new(&pattern[..]).map_err(|e|format!("{}", e))?),
		};

		let mut fields = vec![];

		self.hash_lock(key, |hash| -> ExecResult {
			let end = start + max_check;
			let mut next = start;
			for i in start..end {
				next = i;
				if let Some((k, _)) = hash.get_index(i) {
					if let Some(pattern) = &pattern {
						match k {
							Value::Buffer(k) => {
								if ! pattern.is_match(&k[..]) {
									continue;
								}
							},
							o@_ => {
								let bytes = format!("{}", o).bytes().collect::<Vec<u8>>();
								if ! pattern.is_match(&bytes[..]) {
									continue;
								}
							}
						}
					}
					fields.push(k.clone());
				} else {
					next = 0;
					break;
				}
			}

			let next = Value::Integer(next as i64);
			let fields = Value::Array(
				fields
				.drain(..)
				.collect()
			);
			Ok(Value::Array(vec![next, fields].into()))
		}).await
	}
}

