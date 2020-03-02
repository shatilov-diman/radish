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

mod container;
mod strings;
mod system;
mod expire;
mod list;
mod keys;
mod hash;
mod set;

use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::Mutex;
use indexmap::IndexMap;

use container::ContainersPtr;

pub type Key = radish_types::Key;
pub type Value = radish_types::Value;
pub type Arguments = radish_types::Arguments;
pub type ExecResult = radish_types::ExecResult;
pub type Command = radish_types::Command;

#[derive(Clone)]
pub struct Storage {
	containers: ContainersPtr,
	expire_controller: Arc<Mutex<expire::ExpireController>>,
	expire_awaker: Arc<Mutex<Option<Box<dyn FnMut(SystemTime) + Send + 'static>>>>,
}

impl Storage {
	pub fn new() -> Self {
		Self {
			containers: Arc::new(Mutex::new(IndexMap::new())),
			expire_controller: Arc::new(Mutex::new(expire::ExpireController::new())),
			expire_awaker: Arc::new(Mutex::new(None)),
		}
	}

	pub fn set_expire_awaker<A>(&mut self, a: A)
	where A: FnMut(SystemTime) + Send + 'static {
		self.expire_awaker = Arc::new(Mutex::new(Some(Box::new(a))));
	}

	pub async fn unimplemented(&self) -> ExecResult {
		Err("Unimplemented".to_owned())
	}

	pub async fn execute(&mut self, command: Command) -> Value {
		let result = match &command.command.to_uppercase()[..] {
			"NOW" => self.keys_now(command.arguments).await,
			"PNOW" => self.keys_pnow(command.arguments).await,
			"DEL" => self.keys_del(command.arguments).await,
			"KEYS" => self.keys_keys(command.arguments).await,
			"EXISTS" => self.keys_exists(command.arguments).await,
			"RENAME" => self.keys_rename(command.arguments).await,
			"DUMP" => self.unimplemented().await,
			"EXPIRE" => self.keys_expire(command.arguments).await,
			"EXPIREAT" => self.keys_expire_at(command.arguments).await,
			"MIGRATE" => self.unimplemented().await,
			"MOVE" => self.unimplemented().await,
			"OBJECT" => self.unimplemented().await,
			"PERSIST" => self.unimplemented().await,
			"PEXPIRE" => self.keys_pexpire(command.arguments).await,
			"PEXPIREAT" => self.keys_pexpire_at(command.arguments).await,
			"PTTL" => self.keys_pttl(command.arguments).await,
			"RANDOMKEY" => self.unimplemented().await,
			"RENAMENX" => self.unimplemented().await,
			"RESTORE" => self.unimplemented().await,
			"SORT" => self.unimplemented().await,
			"TOUCH" => self.unimplemented().await,
			"TTL" => self.keys_ttl(command.arguments).await,
			"TYPE" => self.keys_type(command.arguments).await,
			"UNLINK" => self.unimplemented().await,
			"WAIT" => self.unimplemented().await,
			"SCAN" => self.keys_scan(command.arguments).await,

			"APPEND" => self.strings_append(command.arguments).await,
			"GET" => self.strings_get(command.arguments).await,
			"GETSET" => self.strings_getset(command.arguments).await,
			"STRLEN" => self.strings_len(command.arguments).await,
			"BITCOUNT" => self.strings_bitcount(command.arguments).await,
			"BITFIELD" => self.unimplemented().await,
			"BITOP" => self.strings_bitop(command.arguments).await,
			"BITPOS" => self.unimplemented().await,
			"DECR" => self.strings_decrby(command.arguments).await,
			"DECRBY" => self.strings_decrby(command.arguments).await,
			"GETBIT" => self.strings_getbit(command.arguments).await,
			"GETRANGE" => self.strings_get_range(command.arguments).await,
			"INCR" => self.strings_incrby(command.arguments).await,
			"INCRBY" => self.strings_incrby(command.arguments).await,
			"INCRBYFLOAT" => self.strings_incrby_float(command.arguments).await,
			"MGET" => self.strings_mget(command.arguments).await,
			"MSET" => self.strings_mset(command.arguments).await,
			"MSETNX" => self.unimplemented().await,
			"PSETEX" => self.strings_psetex(command.arguments).await,
			"SET" => self.strings_set(command.arguments).await,
			"SETBIT" => self.strings_setbit(command.arguments).await,
			"SETEX" => self.strings_setex(command.arguments).await,
			"SETNX" => self.strings_setnx(command.arguments).await,
			"SETRANGE" => self.strings_set_range(command.arguments).await,

			"LLEN" => self.list_len(command.arguments).await,
			"LPOP" => self.list_lpop(command.arguments).await,
			"RPOP" => self.list_rpop(command.arguments).await,
			"LREM" => self.list_rem(command.arguments).await,
			"LSET" => self.list_set(command.arguments).await,
			"LPUSH" => self.list_lpush(command.arguments).await,
			"RPUSH" => self.list_rpush(command.arguments).await,
			"LPUSHX" => self.list_lpushx(command.arguments).await,
			"RPUSHX" => self.list_rpushx(command.arguments).await,
			"LINDEX" => self.list_index(command.arguments).await,
			"LRANGE" => self.list_range(command.arguments).await,
			"LINSERT" => self.list_insert(command.arguments).await,
			"LTRIM" => self.list_trim(command.arguments).await,
			"RPOPLPUSH" => self.unimplemented().await,
			"BRPOP" => self.unimplemented().await,
			"BLPOP" => self.unimplemented().await,
			"BRPOPLPUSH" => self.unimplemented().await,

			"SADD" => self.set_add(command.arguments).await,
			"SREM" => self.set_rem(command.arguments).await,
			"SPOP" => self.set_pop(command.arguments).await,
			"SSCAN" => self.set_scan(command.arguments).await,
			"SCARD" => self.set_card(command.arguments).await,
			"SMOVE" => self.set_move(command.arguments).await,
			"SMEMBERS" => self.set_members(command.arguments).await,
			"SISMEMBER" => self.set_is_member(command.arguments).await,
			"SDIFF" => self.set_diff(command.arguments).await,
			"SINTER" => self.set_inter(command.arguments).await,
			"SUNION" => self.set_union(command.arguments).await,
			"SDIFFSTORE" => self.set_diff_store(command.arguments).await,
			"SINTERSTORE" => self.set_inter_store(command.arguments).await,
			"SUNIONSTORE" => self.set_union_store(command.arguments).await,
			"SRANDMEMBER" => self.unimplemented().await,

			"HSET" => self.hash_set(command.arguments).await,
			"HSETNX" => self.hash_set_nx(command.arguments).await,
			"HDEL" => self.hash_del(command.arguments).await,
			"HGET" => self.hash_get(command.arguments).await,
			"HGETALL" => self.hash_get_all(command.arguments).await,
			"HEXISTS" => self.hash_exists(command.arguments).await,
			"HKEYS" => self.hash_keys(command.arguments).await,
			"HVALUES" => self.hash_values(command.arguments).await,
			"HLEN" => self.hash_len(command.arguments).await,
			"HSTRLEN" => self.hash_strlen(command.arguments).await,
			"HINCRBY" => self.hash_incrby(command.arguments).await,
			"HINCRBYFLOAT" => self.hash_incrbyfloat(command.arguments).await,
			"HMGET" => self.hash_mget(command.arguments).await,
			"HMSET" => self.hash_set(command.arguments).await,
			"HSCAN" => self.hash_scan(command.arguments).await,

			"AUTHORS" => self.authors(command.arguments).await,
			"VERSION" => self.version(command.arguments).await,
			"LICENSE" => self.license(command.arguments).await,
			"HELP" | "" => self.help(command.arguments).await,
			_ => Err(format!("Unsupported command")),
		};
		match result {
			Ok(r) => r,
			Err(err) => Value::Error(err),
		}
	}
}

