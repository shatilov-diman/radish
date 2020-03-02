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

const REPOSITORY:   Option<&'static str> = option_env!("CARGO_PKG_REPOSITORY");
const LICENSE:      Option<&'static str> = option_env!("CARGO_PKG_LICENSE");
const VERSION:      Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const AUTHORS:      Option<&'static str> = option_env!("CARGO_PKG_AUTHORS");
const NAME:         Option<&'static str> = option_env!("CARGO_PKG_NAME");

type Value = super::Value;
type Arguments = super::Arguments;
type ExecResult = super::ExecResult;

impl super::Storage {
	pub async fn authors(&self, mut _args: Arguments) -> ExecResult {
		let ath = format!("{} copyright @ 2020 {}", NAME.unwrap_or("Radish"), AUTHORS.unwrap_or("Dmitry Shatilov <shatilov dot diman at gmail dot com>"));
		Ok(Value::Buffer(ath.bytes().collect()))
	}
	pub async fn version(&self, mut _args: Arguments) -> ExecResult {
		let ver = VERSION.unwrap_or("Custom");
		Ok(Value::Buffer(ver.bytes().collect()))
	}
	pub async fn license(&self, mut _args: Arguments) -> ExecResult {
		let lic = LICENSE.unwrap_or("AGPL v.3");
		Ok(Value::Buffer(lic.bytes().collect()))
	}
	pub async fn help(&self, mut _args: Arguments) -> ExecResult {
		let hlp = format!("Under construction. Please see {} for help", REPOSITORY.unwrap_or("internet"));
		Ok(Value::Buffer(hlp.bytes().collect()))
	}
}

