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

use indexmap::IndexSet;

use super::container::Container;
use super::container::ContainerPtr;
use super::container::ContainerImpl;

type Key = super::Key;
type Value = super::Value;
type Arguments = super::Arguments;
type ExecResult = super::ExecResult;

type Inner = IndexSet<Value>;

impl super::Storage {
	async fn set_get_container(&self, key: Key) -> ContainerPtr {
		self.get_container(key, ||Container::Set(ContainerImpl::<Inner>::new())).await
	}
	async fn set_get_containers(&self, keys: Vec<Key>) -> Vec<ContainerPtr> {
		self.get_containers(keys, ||Container::Set(ContainerImpl::<Inner>::new())).await
	}
	async fn set_unwrap_container(container: &Container) -> Result<&ContainerImpl<Inner>, String> {
		match container {
			Container::Set(ref c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn set_unwrap_mut_container(container: &mut Container) -> Result<&mut ContainerImpl<Inner>, String> {
		match container {
			Container::Set(ref mut c) => Ok(c),
			_ => Err(format!("Unexpected container type")),
		}
	}
	async fn set_lock<F: FnOnce(&Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.set_get_container(key).await;
		let c2 = c1.read().await;
		let c3 = Self::set_unwrap_container(&c2).await?;
		processor(&c3.inner)
	}
	async fn set_lock_mut<F: FnOnce(&mut Inner) -> ExecResult>(&self, key: Key, processor: F) -> ExecResult {
		let c1 = self.set_get_container(key).await;
		let mut c2 = c1.write().await;
		let c3 = Self::set_unwrap_mut_container(&mut c2).await?;
		processor(&mut c3.inner)
	}

	async fn set_lock_containers<F>(&self, keys: Vec<Key>, callback: F) -> ExecResult
	where F: FnOnce(VecDeque<&mut ContainerImpl<Inner>>) -> ExecResult {
		let containers = self.set_get_containers(keys).await;
		let (mut guards, _) = Self::lock_all(containers.iter().map(|c|c.as_ref()), std::iter::empty()).await;

		let mut inners = VecDeque::with_capacity(guards.len());
		for g in &mut guards {
			inners.push_back(Self::set_unwrap_mut_container(&mut *g).await?);
		}

		callback(inners)
	}

	pub async fn set_card(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.set_lock(key, |set| -> ExecResult {
			Ok(Value::Integer(set.len() as i64))
		}).await
	}

	pub async fn set_members(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.set_lock(key, |set| -> ExecResult {
			Ok(Value::Array(set.iter().map(|v|v.clone()).collect()))
		}).await
	}

	pub async fn set_is_member(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let member = Self::extract(args.pop_front())?;
		self.set_lock(key, |set| -> ExecResult {
			Ok(Value::Integer(if set.contains(&member) {1} else {0}))
		}).await
	}

	pub async fn set_add(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.set_lock_mut(key, |set| -> ExecResult {
			let mut count: u32 = 0;
			for arg in args {
				if set.insert(arg) {
					count = count + 1;
				}
			}
			Ok(Value::Integer(count as i64))
		}).await
	}

	pub async fn set_rem(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		self.set_lock_mut(key, |set| {
			let mut count: u32 = 0;
			for arg in args {
				if set.remove(&arg) {
					count = count + 1;
				}
			}
			Ok(Value::Integer(count as i64))
		}).await
	}

	pub async fn set_pop(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let count = if let Ok(count) = Self::extract_index(args.pop_front()) {count} else {1};
		self.set_lock_mut(key, |set| {
			let mut remove_items = VecDeque::with_capacity(count);
			for _ in 0..count {
				let index = rand::random::<usize>() % set.len();
				if let Some(item) = set.swap_remove_index(index) {
					remove_items.push_back(item);
				}
			}
			Ok(Value::Array(remove_items))
		}).await
	}

	pub async fn set_move(&self, mut args: Arguments) -> ExecResult {
		let source = Self::extract_key(args.pop_front())?;
		let destination = Self::extract_key(args.pop_front())?;
		let member = Self::extract(args.pop_front())?;
		self.set_lock_containers(vec![source, destination], |mut sets| -> ExecResult {
			let source = sets.pop_front().unwrap();
			if ! source.inner.remove(&member) {
				Ok(Value::Integer(0))
			} else {
				let destination = sets.pop_front().unwrap();
				destination.inner.insert(member);
				Ok(Value::Integer(1))
			}
		}).await
	}

	fn set_diff_make_iter<'a>(sets: &'a VecDeque<&mut ContainerImpl<Inner>>) -> impl Iterator<Item=Value> + 'a {
		let main_set = sets.get(0).unwrap();
		main_set
		.inner
		.iter()
		.filter(move |&v| {
			! sets
			.iter()
			.skip(1)
			.any(|set| set.inner.contains(v))
		})
		.map(|v|v.clone())
	}

	pub async fn set_diff(&self, mut args: Arguments) -> ExecResult {
		let mut keys = vec![Self::extract_key(args.pop_front())?];
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			keys.push(key);
		}
		self.set_lock_containers(keys, |sets| -> ExecResult {
			Ok(Value::Array(Self::set_diff_make_iter(&sets).collect()))
		}).await
	}

	pub async fn set_diff_store(&self, mut args: Arguments) -> ExecResult {
		let mut keys = vec![Self::extract_key(args.pop_front())?];
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			keys.push(key);
		}
		self.set_lock_containers(keys, |mut sets| -> ExecResult {
			let dest_set = sets.pop_front().unwrap();

			let mut tmp = Inner::new();
			Self::set_diff_make_iter(&sets).for_each(|v|{tmp.insert(v.clone());});

			dest_set.inner.clear();
			dest_set.expiration_time = None;
			std::mem::swap(&mut dest_set.inner, &mut tmp);

			Ok(Value::Integer(dest_set.inner.len() as i64))
		}).await
	}

	fn set_inter_make_iter<'a>(sets: &'a VecDeque<&mut ContainerImpl<Inner>>) -> impl Iterator<Item=Value> + 'a {
		let main_set = sets.get(0).unwrap();
		main_set
		.inner
		.iter()
		.filter(move |&v| {
			! sets
			.iter()
			.skip(1)
			.any(|set| ! set.inner.contains(v))
		})
		.map(|v|v.clone())
	}

	pub async fn set_inter(&self, mut args: Arguments) -> ExecResult {
		let mut keys = vec![Self::extract_key(args.pop_front())?];
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			keys.push(key);
		}
		self.set_lock_containers(keys, |sets| -> ExecResult {
			Ok(Value::Array(Self::set_inter_make_iter(&sets).collect()))
		}).await
	}

	pub async fn set_inter_store(&self, mut args: Arguments) -> ExecResult {
		let mut keys = vec![Self::extract_key(args.pop_front())?];
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			keys.push(key);
		}
		self.set_lock_containers(keys, |mut sets| -> ExecResult {
			let dest_set = sets.pop_front().unwrap();

			let mut tmp = Inner::new();
			Self::set_inter_make_iter(&sets).for_each(|v|{tmp.insert(v.clone());});

			dest_set.inner.clear();
			dest_set.expiration_time = None;
			std::mem::swap(&mut dest_set.inner, &mut tmp);

			Ok(Value::Integer(dest_set.inner.len() as i64))
		}).await
	}

	fn set_union_make_iter<'a>(sets: &'a VecDeque<&mut ContainerImpl<Inner>>) -> impl Iterator<Item=Value> + 'a {
		sets
		.iter()
		.flat_map(|s|s.inner.iter())
		.map(|v|v.clone())
	}

	pub async fn set_union(&self, mut args: Arguments) -> ExecResult {
		let mut keys = vec![Self::extract_key(args.pop_front())?];
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			keys.push(key);
		}
		self.set_lock_containers(keys, |sets| -> ExecResult {
			let mut tmp = Inner::new();
			Self::set_union_make_iter(&sets).for_each(|v|{tmp.insert(v.clone());});
			Ok(Value::Array(tmp.drain(..).collect()))
		}).await
	}

	pub async fn set_union_store(&self, mut args: Arguments) -> ExecResult {
		let mut keys = vec![Self::extract_key(args.pop_front())?];
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			keys.push(key);
		}
		self.set_lock_containers(keys, |mut sets| -> ExecResult {
			let dest_set = sets.pop_front().unwrap();

			let mut tmp = Inner::new();
			Self::set_union_make_iter(&sets).for_each(|v|{tmp.insert(v.clone());});

			dest_set.inner.clear();
			dest_set.expiration_time = None;
			std::mem::swap(&mut dest_set.inner, &mut tmp);

			Ok(Value::Integer(dest_set.inner.len() as i64))
		}).await
	}

	pub async fn _set_rand_member(&self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let count = if let Ok(count) = Self::extract_integer(args.pop_front()) {count} else {1};
		let (repeates, count) = if count >= 0 {(false, count as usize)} else {(true, -count as usize)};

		self.set_lock_mut(key, |set| {
			let mut items = VecDeque::with_capacity(count);

			if repeates {
				for _ in 0..count {
					let index = rand::random::<usize>() % set.len();
					if let Some(item) = set.get_index(index) {
						items.push_back(item.clone());
					}
				}
			} else {
				return Err("Unimplemented".to_owned());
			}
			Ok(Value::Array(items))
		}).await
	}

	pub async fn set_scan(&self, mut args: Arguments) -> ExecResult {
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

		let mut values = vec![];

		self.set_lock(key, |set| -> ExecResult {
			let end = start + max_check;
			let mut next = end;
			for i in start..end {
				if let Some(value) = set.get_index(i) {
					if let Some(pattern) = &pattern {
						match value {
							Value::Buffer(value) => {
								if ! pattern.is_match(&value[..]) {
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
					values.push(value.clone());
				} else {
					next = 0;
					break;
				}
			}

			let next = Value::Integer(next as i64);
			let values = Value::Array(
				values
				.drain(..)
				.collect()
			);
			Ok(Value::Array(vec![next, values].into()))
		}).await
	}
}

