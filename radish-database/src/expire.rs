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

use std::time::{SystemTime, Duration};
use std::collections::{BTreeMap, HashSet};

type Key = super::Key;

pub struct ExpireController {
	expires_queue: BTreeMap<SystemTime, HashSet<Key>>,
}

impl ExpireController {
	pub fn new() -> Self {
		Self {
			expires_queue: BTreeMap::new(),
		}
	}

	pub fn pop_now_and_expired_keys(&mut self) -> (SystemTime, HashSet<Key>) {
		let pivot = SystemTime::now() + Duration::from_micros(1);
		let tail = self.expires_queue.split_off(&pivot);
		let mut expireds = std::mem::replace(&mut self.expires_queue, tail);
		log::debug!("{:?} && {:?}", expireds, self.expires_queue);

		let mut times = expireds.keys().cloned().collect::<Vec<SystemTime>>();
		let mut out_keys = HashSet::new();
		while let Some(time) = times.pop() {
			if let Some(keys) = expireds.remove(&time) {
				for key in keys {
					out_keys.insert(key);
				}
			}
		}
		(pivot, out_keys)
	}

	pub fn expire_key_at(&mut self, key: &Key, timepoint: SystemTime) {
		log::debug!("{:?}: will expired at {:?}", key, timepoint);

		let keys = self.expires_queue.entry(timepoint).or_insert_with(||HashSet::new());
		keys.insert(key.clone());
	}
}

impl super::Storage {

	pub async fn expire_key_at(&mut self, key: &Key, timepoint: SystemTime) {
		let mut controller = self.expire_controller.lock().await;
		controller.expire_key_at(key, timepoint);
		drop(controller);
		let awaker = self.expire_awaker.clone();
		let mut awaker = awaker.lock().await;
		if let Some(awaker) = &mut *awaker {
			(*awaker)(timepoint);
		}
	}
}

