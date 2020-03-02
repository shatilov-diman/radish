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
use std::collections::VecDeque;

use tokio::sync::RwLock;
use indexmap::{IndexSet, IndexMap};

type Key = super::Key;
type Value = super::Value;

#[derive(Debug)]
pub struct ContainerImpl<Inner> {
	pub inner: Inner,
	pub expiration_time: Option<std::time::SystemTime>,
}
impl<Inner: Default> ContainerImpl<Inner> {
	pub fn new() -> Self {
		Self {
			inner: Inner::default(),
			expiration_time: None,
		}
	}
}

#[derive(Debug)]
pub enum Container {
	Set(ContainerImpl<IndexSet<Value>>),
	List(ContainerImpl<VecDeque<Value>>),
	Hash(ContainerImpl<IndexMap<Value, Value>>),
	Strings(ContainerImpl<Vec<u8>>),
}
pub type ContainerPtr = Arc<RwLock<Container>>;
pub type Containers = IndexMap<Key, ContainerPtr>;
pub type ContainersPtr = Arc<RwLock<Containers>>;


impl super::Storage {

	pub fn extract(arg: Option<Value>) -> Result<Value, String> {
		match arg {
			Some(arg) => Ok(arg),
			None => Err(format!("Not enough arguments")),
		}
	}

	pub fn extract_buffer(arg: Option<Value>) -> Result<Vec<u8>, String> {
		match Self::extract(arg)? {
			Value::Buffer(k) => Ok(k),
			_ => Err(format!("Unexpected buffer type")),
		}
	}

	pub fn extract_string(arg: Option<Value>) -> Result<String, String> {
		match String::from_utf8(Self::extract_buffer(arg)?) {
			Ok(s) => Ok(s),
			Err(err) => Err(format!("Failed to extract string: {}", err)),
		}
	}

	pub fn extract_key(arg: Option<Value>) -> Result<Key, String> {
		match Self::extract(arg)? {
			Value::Buffer(k) => Ok(k),
			_ => Err(format!("Unexpected key type")),
		}
	}

	pub fn extract_integer(arg: Option<Value>) -> Result<i64, String> {
		match Self::extract(arg)? {
			Value::Integer(i) => Ok(i),
			_ => Err(format!("{}", "Unexpected index type")),
		}
	}

	pub fn extract_unsigned_integer(arg: Option<Value>) -> Result<u64, String> {
		match Self::extract(arg)? {
			Value::Integer(i) => Ok(i as u64),
			_ => Err(format!("{}", "Unexpected index type")),
		}
	}

	pub fn extract_float(arg: Option<Value>) -> Result<f64, String> {
		match Self::extract(arg)? {
			Value::Float(n) => Ok(f64::from_bits(n)),
			_ => Err(format!("{}", "Unexpected index type")),
		}
	}

	pub fn extract_index(arg: Option<Value>) -> Result<usize, String> {
		match Self::extract(arg)? {
			Value::Integer(i) => num::cast(i).ok_or(format!("Index is out of range: [0; {}]", usize::max_value())),
			_ => Err(format!("{}", "Unexpected index type")),
		}
	}

	pub fn extract_bit(arg: Option<Value>) -> Result<bool, String> {
		match Self::extract(arg)? {
			Value::Bool(b) => Ok(b),
			Value::Integer(i) => match i {
				0 => Ok(false),
				1 => Ok(true),
				_ => Err(format!("Unexpected bit value")),
			},
			_ => Err(format!("Unexpected bit type")),
		}
	}
}

