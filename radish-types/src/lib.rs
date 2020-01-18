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

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub type Key = Vec<u8>;
pub type Arguments = VecDeque<Value>;
pub type ExecResult = Result<Value, String>;

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub enum Value {
	Nill,
	Ok,
	Bool(bool),
	Integer(i64),
	Float(u64),
	Buffer(Vec<u8>),
	Array(VecDeque<Value>),
	Error(String),
}


#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Command {
	pub command: String,
	pub arguments: Arguments,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct CommandResult {
	pub results: Value,
}

impl std::fmt::Display for Value {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Value::Nill => write!(f, "nill"),
			Value::Ok => write!(f, "ok"),
			Value::Bool(false) => write!(f, "false"),
			Value::Bool(true) => write!(f, "true"),
			Value::Integer(i) => write!(f, "{}", i),
			Value::Float(i) => write!(f, "{}", f64::from_bits(*i)),
			Value::Buffer(v) => write!(f, "{:?}", v),
			Value::Array(v) => write!(f, "{:?}", v),
			Value::Error(m) => write!(f, "{}", m),
		}
	}
}

impl std::fmt::Display for Command {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let args =
			self
			.arguments
			.iter()
			.map(|arg|format!("{}", arg))
			.collect::<Vec<String>>()
			.join(",")
		;
		write!(f, "{}: [{}]", self.command, args)
	}
}

