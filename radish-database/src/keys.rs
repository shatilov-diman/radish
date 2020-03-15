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

use std::sync::Arc;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{SystemTime, Duration};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::container::Container;
use super::container::ContainerPtr;

type Key = super::Key;
type Value = super::Value;
type Arguments = super::Arguments;
type ExecResult = super::ExecResult;

enum RwAction<'a, T> {
	WriteLock(&'a RwLock<T>),
	ReadLock(&'a RwLock<T>),
}

impl super::Storage {
	pub fn make_container(cnt: Container) -> ContainerPtr {
		Arc::new(RwLock::new(cnt))
	}
	pub fn make_container_with<F: FnMut() -> Container>(mut factory: F) -> ContainerPtr {
		Self::make_container(factory())
	}

	pub async fn try_get_container(&self, key: &Key) -> Option<ContainerPtr> {
		let containers = self.containers.read().await;
		containers
		.get(key)
		.cloned()
	}

	pub async fn get_container<F: FnMut() -> Container>(&self, key: Key, factory: F) -> ContainerPtr {
		match self.try_get_container(&key).await {
			Some(c) => c,
			None => {
				let mut containers = self.containers.write().await;
				containers
				.entry(key.clone())
				.or_insert_with(||Self::make_container_with(factory))
				.clone()
			}
		}
	}

	pub async fn try_get_containers(&self, keys: &Vec<Key>) -> Vec<Option<ContainerPtr>> {
		let containers = self.containers.read().await;

		keys
		.iter()
		.map(|key| {
			match containers.get(key) {
				Some(v) => Some(v.clone()),
				None => None,
			}
		})
		.collect()
	}

	pub async fn get_containers<F: FnMut() -> Container>(&self, mut keys: Vec<Key>, mut factory: F) -> Vec<ContainerPtr> {
		let mut containers = self.containers.write().await;

		keys
		.drain(..)
		.map(|key| {
			if let Some(v) = containers.get(&key) {
				v.clone()
			} else {
				let c = Self::make_container_with(||factory());
				containers.insert(key, c.clone());
				c
			}
		})
		.collect()
	}

	pub async fn lock_all<'a, T: 'a>(mut writes: impl Iterator<Item=&'a RwLock<T>>, mut reads: impl Iterator<Item=Option<&'a RwLock<T>>>) -> (Vec<RwLockWriteGuard<'a, T>>, Vec<Option<RwLockReadGuard<'a, T>>>) {
		let mut locks = BTreeMap::<u64, RwAction<'a, T>>::new();
		let mut guard_writes = HashMap::<u64, RwLockWriteGuard<'a, T>>::new();
		let mut guard_reads = HashMap::<u64, RwLockReadGuard<'a, T>>::new();
		let mut output_order_writes = Vec::<u64>::new();
		let mut output_order_reads = Vec::<u64>::new();
		while let Some(w) = writes.next() {
			let address = w as *const RwLock<T> as u64;
			locks.insert(address, RwAction::<'a, T>::WriteLock(w));
			output_order_writes.push(address);
		}
		while let Some(m) = reads.next() {
			match m {
				None => output_order_reads.push(0),
				Some(w) => {
					let address = w as *const RwLock<T> as u64;
					locks.insert(address, RwAction::<'a, T>::ReadLock(w));
					output_order_reads.push(address);
				},
			}
		}
		for (address, l) in locks {
			match l {
				RwAction::<'a, T>::WriteLock(l) => {guard_writes.insert(address, l.write().await);},
				RwAction::<'a, T>::ReadLock(l) => {guard_reads.insert(address, l.read().await);},
			}
		}
		let writes = output_order_writes
			.iter()
			.map(|a|guard_writes.remove(a).unwrap())
			.collect()
		;
		let reads = output_order_reads
			.iter()
			.map(|a|{
				match a {
					0 => None,
					a => Some(guard_reads.remove(a).unwrap()),
				}
			})
			.collect()
		;
		(writes, reads)
	}

	pub async fn keys_keys(&self, mut args: Arguments) -> ExecResult {
		let pattern = Self::extract_key(args.pop_front())?;
		let pattern = std::str::from_utf8(&pattern[..]).map_err(|e|format!("{}", e))?;
		let pattern = regex::bytes::Regex::new(pattern).map_err(|e|format!("{}", e))?;
		let filter = |key: &&Key| -> bool {
			pattern.is_match(&key[..])
		};

		let containers = self.containers.read().await;

		Ok(Value::Array(
			containers
			.keys()
			.filter(filter)
			.map(|key| Value::Buffer(key.clone()))
			.collect()
		))
	}

	pub async fn keys_exists(&self, mut args: Arguments) -> ExecResult {
		let containers = self.containers.read().await;

		let mut exists_count = 0;
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			if let Some(_) = containers.get(&key) {
				exists_count = exists_count + 1;
			}
		}
		Ok(Value::Integer(exists_count))
	}

	pub async fn keys_now(&self, _args: Arguments) -> ExecResult {
		let timepoint = SystemTime::now();
		let timestamp = timepoint.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
		Ok(Value::Integer(timestamp as i64))
	}

	pub async fn keys_pnow(&self, _args: Arguments) -> ExecResult {
		let timepoint = SystemTime::now();
		let timestamp = timepoint.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
		Ok(Value::Integer(timestamp as i64))
	}

	pub async fn keys_del(&self, mut args: Arguments) -> ExecResult {
		let mut containers = self.containers.write().await;

		let mut removed_count = 0;
		while let Ok(key) = Self::extract_key(args.pop_front()) {
			if let Some(_) = containers.remove(&key) {
				removed_count = removed_count + 1;
			}
		}
		Ok(Value::Integer(removed_count))
	}

	async fn key_expiration(&self, cnt: &ContainerPtr) -> Option<std::time::SystemTime> {
		let cnt = cnt.read().await;
		match &*cnt {
			Container::Set(c) => c.expiration_time,
			Container::List(c) => c.expiration_time,
			Container::Hash(c) => c.expiration_time,
			Container::Strings(c) => c.expiration_time,
		}
	}

	pub async fn keys_rename(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let newkey = Self::extract_key(args.pop_front())?;

		let mut containers = self.containers.write().await;
		let cnt = containers.remove(&key).ok_or_else(||format!("key '{:?}' not found", &key[..]))?;
		let timepoint = self.key_expiration(&cnt).await;
		containers.insert(newkey.clone(), cnt);
		drop(containers);

		if let Some(timepoint) = timepoint {
			self.expire_key_at(&newkey, timepoint).await;
		}
		Ok(Value::Ok)
	}

	fn check_type(key_type: &str) -> Result<(), String> {
		match key_type {
			"set" => Ok(()),
			"list" => Ok(()),
			"hash" => Ok(()),
			"string" => Ok(()),
			t => Err(format!("Unexpected type '{}'", t)),
		}
	}

	fn type_to_string(c: &Container) -> &str {
		match *c {
			Container::Set(_) => "set",
			Container::List(_) => "list",
			Container::Hash(_) => "hash",
			Container::Strings(_) => "string",
		}
	}

	pub async fn keys_type(&self, mut args: Arguments) -> ExecResult {
		let keys = args.drain(..).filter_map(|a|Self::extract_key(Some(a)).ok()).collect();
		let cnts = self.try_get_containers(&keys).await;
		let mut types = VecDeque::new();
		for c in cnts {
			let ktype = match c {
				None => Value::Nill,
				Some(c) => {
					let c = c.read().await;
					let t = Self::type_to_string(&c);
					Value::Buffer(Vec::from(t.as_bytes()))
				}
			};
			types.push_back(ktype);
		}
		match types.len() {
			0 => Err(format!("TYPE key")),
			1 => Ok(types.remove(0).unwrap()),
			_ => Ok(Value::Array(types)),
		}
	}

	fn get_expiration_time(c: &Container) -> Option<SystemTime> {
		match c {
			Container::Set(c) => c.expiration_time,
			Container::List(c) => c.expiration_time,
			Container::Hash(c) => c.expiration_time,
			Container::Strings(c) => c.expiration_time,
		}
	}
	fn set_expiration_time(c: &mut Container, t: Option<SystemTime>) {
		let expire = match c {
			Container::Set(c) => &mut c.expiration_time,
			Container::List(c) => &mut c.expiration_time,
			Container::Hash(c) => &mut c.expiration_time,
			Container::Strings(c) => &mut c.expiration_time,
		};
		*expire = t;
	}

	async fn keys_expiration_time<F>(&mut self, mut args: Arguments, dur_to_i64: F) -> ExecResult
	where F: FnOnce(Duration)->i64 {
		let key = Self::extract_key(args.pop_front())?;
		match self.try_get_container(&key).await {
			None => Ok(Value::Integer(-2)),
			Some(c) => {
				let c = c.read().await;
				match Self::get_expiration_time(&*c) {
					None => Ok(Value::Integer(-1)),
					Some(tm) => {
						let ttl = tm.duration_since(SystemTime::now()).unwrap_or(Duration::new(0, 0));
						Ok(Value::Integer(dur_to_i64(ttl)))
					},
				}
			}
		}
	}

	pub async fn keys_pttl(&mut self, args: Arguments) -> ExecResult {
		self.keys_expiration_time(args, |ttl|ttl.as_millis() as i64).await
	}

	pub async fn keys_ttl(&mut self, args: Arguments) -> ExecResult {
		self.keys_expiration_time(args, |ttl|ttl.as_secs() as i64).await
	}

	async fn keys_expire_impl(&mut self, key: Key, timepoint: SystemTime) -> ExecResult {
		let c = self.try_get_container(&key).await;
		match c {
			None => Ok(Value::Bool(false)),
			Some(c) => {
				let mut c = c.write().await;
				Self::set_expiration_time(&mut *c, Some(timepoint));
				drop(c);
				self.expire_key_at(&key, timepoint).await;
				Ok(Value::Bool(true))
			},
		}
	}

	pub async fn keys_expire(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let seconds = Self::extract_unsigned_integer(args.pop_front())?;
		let timepoint = SystemTime::now() + Duration::from_secs(seconds);
		self.keys_expire_impl(key, timepoint).await
	}

	pub async fn keys_expire_at(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let seconds = Self::extract_unsigned_integer(args.pop_front())?;
		let timepoint = SystemTime::UNIX_EPOCH + Duration::from_secs(seconds);
		self.keys_expire_impl(key, timepoint).await
	}

	pub async fn keys_pexpire(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let millis = Self::extract_unsigned_integer(args.pop_front())?;
		let timepoint = SystemTime::now() + Duration::from_millis(millis);
		self.keys_expire_impl(key, timepoint).await
	}

	pub async fn keys_pexpire_at(&mut self, mut args: Arguments) -> ExecResult {
		let key = Self::extract_key(args.pop_front())?;
		let millis = Self::extract_unsigned_integer(args.pop_front())?;
		let timepoint = SystemTime::UNIX_EPOCH + Duration::from_millis(millis);
		self.keys_expire_impl(key, timepoint).await
	}

	pub async fn keys_check_expirations(&self) {
		log::debug!("Begin expiration check");

		let (now, expired) = {
			let mut controller = self.expire_controller.lock().await;
			controller.pop_now_and_expired_keys()
		};

		log::debug!("{:?}: {:?}", now, expired);

		for key in expired {
			if let Some(c) = self.try_get_container(&key).await {
				let c = c.read().await;
				let tm = Self::get_expiration_time(&*c);
				log::debug!("{:?}: {:?} vs {:?}", key, tm, now);
				match tm {
					Some(time) => {
						if time > now {
							log::warn!("{:?}: will removed at {:?}", key, time);
						} else {
							log::debug!("{:?}: expired and removed", key);
							let mut containers = self.containers.write().await;
							containers.remove(&key);
						}
					},
					None => (),
				}
			}
		}
		log::debug!("Check expiration done");
	}

	pub async fn keys_scan(&self, mut args: Arguments) -> ExecResult {
		let start = Self::extract_index(args.pop_front())?;

		let mut pattern: Option<String> = None;
		let mut key_type: Option<String> = None;
		let mut max_check = 100usize;

		while let Some(subcmd) = Self::extract_string(args.pop_front()).ok() {
			match &subcmd.to_uppercase()[..] {
				"MATCH" => pattern = Some(Self::extract_string(args.pop_front())?),
				"COUNT" => max_check = Self::extract_index(args.pop_front())?,
				"TYPE" => key_type = Some(Self::extract_string(args.pop_front())?),
				arg => return Err(format!("Unexpected argument '{}'", arg)),
			}
		}
		if let Some(key_type) = &key_type {
			Self::check_type(&key_type[..])?;
		}

		let pattern = match pattern {
			None => None,
			Some(pattern) => Some(regex::bytes::Regex::new(&pattern[..]).map_err(|e|format!("{}", e))?),
		};

		let containers = self.containers.read().await;

		let mut keys = vec![];

		let end = start + max_check;
		let mut next = end;
		for i in start..end {
			if let Some((key, container)) = containers.get_index(i) {
				if let Some(key_type) = &key_type {
					let container = container.read().await;
					let t = Self::type_to_string(&container);
					if key_type != t {
						continue;
					}
				}
				if let Some(pattern) = &pattern {
					if ! pattern.is_match(&key[..]) {
						continue;
					}
				}
				keys.push(key.clone());
			} else {
				next = 0;
				break;
			}
		}

		let next = Value::Integer(next as i64);
		let keys = Value::Array(
			keys
			.drain(..)
			.map(|key| Value::Buffer(key))
			.collect()
		);
		Ok(Value::Array(vec![next, keys].into()))
	}
}

