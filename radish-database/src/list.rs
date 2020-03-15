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

use super::container::Container;
use super::container::ContainerPtr;
use super::container::ContainerImpl;

type Key = super::Key;
type Value = super::Value;
type Arguments = super::Arguments;
type ExecResult = super::ExecResult;

type Inner = VecDeque<Value>;

impl super::Storage {
	async fn list_get_container(&self, key: Key) -> ContainerPtr {
		self.get_container(key, ||Container::List(ContainerImpl::<Inner>::new())).await
	}
	async fn list_try_get_container(&self, key: &Key) -> Option<ContainerPtr> {
		self.try_get_container(key).await
	}
	async fn list_unwrap_container(container: &Container) -> Result<&ContainerImpl<Inner>, String> {
		match container {
			Container::List(ref c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn list_unwrap_mut_container(container: &mut Container) -> Result<&mut ContainerImpl<Inner>, String> {
		match container {
			Container::List(ref mut c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn list_lock<F: FnOnce(&Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.list_get_container(key).await;
		let c2 = c1.read().await;
		let c3 = Self::list_unwrap_container(&c2).await?;
		processor(&c3.inner)
	}
	async fn list_lock_mut<F: FnOnce(&mut Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.list_get_container(key).await;
		let mut c2 = c1.write().await;
		let c3 = Self::list_unwrap_mut_container(&mut c2).await?;
		processor(&mut c3.inner)
	}
	async fn list_try_lock_mut<F: FnOnce(&mut Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		match self.list_try_get_container(&key).await {
			None => Ok(Value::Nill),
			Some(c1) => {
				let mut c2 = c1.write().await;
				let c3 = Self::list_unwrap_mut_container(&mut c2).await?;
				processor(&mut c3.inner)
			}
		}
	}

	pub async fn list_len(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_lock(key, |list| -> ExecResult {
			Ok(Value::Integer(list.len() as i64))
		}).await
	}

	pub async fn list_lpush(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			for arg in args {
				list.push_front(arg);
			}
			Ok(Value::Integer(list.len() as i64))
		}).await
	}

	pub async fn list_rpush(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			for arg in args {
				list.push_back(arg);
			}
			Ok(Value::Integer(list.len() as i64))
		}).await
	}

	pub async fn list_lpushx(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_try_lock_mut(key, |list| -> ExecResult {
			for arg in args {
				list.push_front(arg);
			}
			Ok(Value::Integer(list.len() as i64))
		}).await
	}

	pub async fn list_rpushx(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_try_lock_mut(key, |list| -> ExecResult {
			for arg in args {
				list.push_back(arg);
			}
			Ok(Value::Integer(list.len() as i64))
		}).await
	}

	pub async fn list_lpop(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			match list.pop_front() {
				Some(v) => Ok(v),
				None => Ok(Value::Nill),
			}
		}).await
	}

	pub async fn list_rpop(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			match list.pop_back() {
				Some(v) => Ok(v),
				None => Ok(Value::Nill),
			}
		}).await
	}

	pub async fn list_rem(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let index = Self::extract_index(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			match list.remove(index) {
				Some(v) => Ok(v),
				None => Err(format!("{}", "Out of index")),
			}
		}).await
	}

	pub async fn list_set(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let index = Self::extract_index(args.pop_front())?;
		let value = Self::extract(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			match list.get_mut(index) {
				None => Err(format!("{}\r\n", "Out of index")),
				Some(v) => {
					let mut x = value;
					std::mem::swap(v, &mut x);
					Ok(x)
				},
			}
		}).await
	}

	pub async fn list_index(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let index = Self::extract_index(args.pop_front())?;
		self.list_lock(key, |list| -> ExecResult {
			match list.get(index) {
				Some(v) => Ok((*v).clone()),
				None => Err(format!("{}\r\n", "Out of index")),
			}
		}).await
	}

	pub async fn list_range(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let start = Self::extract_index(args.pop_front())?;
		let stop = Self::extract_index(args.pop_front())?;
		self.list_lock(key, |list| -> ExecResult {
			let start = std::cmp::min(start, list.len());
			let end = std::cmp::min(stop+1, list.len());
			let mut out = VecDeque::with_capacity(end - start);
			for i in start..end {
				if let Some(v) = list.get(i) {
					out.push_back((*v).clone());
				}
			}
			Ok(Value::Array(out))
		}).await
	}

	pub async fn list_insert(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let before_after = Self::extract_string(args.pop_front())?;
		let pivot = Self::extract(args.pop_front())?;
		let value = Self::extract(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			let shift = match &before_after.to_lowercase()[..] {
				"before" => 0,
				"after" => 1,
				dir => return Err(format!("Unexpected direction {}", dir)),
			};

			let index = list.iter().position(|v| *v == pivot);
			if let Some(index) = index {
				list.insert(index + shift, value);
				Ok(Value::Integer(list.len() as i64))
			} else {
				Ok(Value::Integer(-1))
			}
		}).await
	}

	pub async fn list_trim(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let start = Self::extract_integer(args.pop_front())?;
		let stop = Self::extract_integer(args.pop_front())?;
		self.list_lock_mut(key, |list| -> ExecResult {
			let start = if start < 0 {list.len() as i64 + start} else {start} as usize;
			let start = std::cmp::min(start, list.len());
			let stop = if stop < 0 {list.len() as i64 + stop} else {stop} as usize;
			let stop = std::cmp::min(stop, list.len());
			if start > stop || start >= list.len() {
				list.clear();
			} else if start > 0 {
				list.rotate_left(start);
				list.truncate(stop+1 - start);
			} else {
				list.truncate(stop+1);
			}

			Ok(Value::Ok)
		}).await
	}

	pub async fn _list_rpop_lpush(&self, mut args: Arguments) -> ExecResult {
		let source = Self::extract_key(args.pop_front())?;
		let destination = Self::extract_key(args.pop_front())?;
		if source == destination {
			self.list_try_lock_mut(source, |list| -> ExecResult {
				if let Some(v) = list.pop_back() {
					list.push_front(v.clone());
					Ok(v)
				} else {
					Ok(Value::Nill)
				}
			}).await
		} else {
			Ok(Value::Error("Should be atomic!!!".to_string()))
		}
	}
}

