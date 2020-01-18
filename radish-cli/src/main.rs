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
use std::convert::TryFrom;
use std::collections::VecDeque;

use tokio::net::TcpStream;
use tokio::io::{BufReader, AsyncReadExt, AsyncBufReadExt, AsyncWriteExt};

use radish_types::*;

fn arg_to_value(arg: &String) -> Value {
	if arg.starts_with("'") && arg.ends_with("'") || arg.starts_with("\"") && arg.ends_with("\"") {
		let bytes = arg.as_bytes();
		return Value::Buffer(Vec::from(&bytes[1..bytes.len()-1]));
	}
	if let Some(_) = arg.find('.') {
		if let Ok(f) = arg.parse::<f64>() {
			return Value::Float(f.to_bits());
		}
	};
	match &arg[..] {
		"nill" => Value::Nill,
		"ok" => Value::Ok,
		"true" => Value::Bool(true),
		"false" => Value::Bool(false),
		arg => match arg.parse::<i64>() {
			Err(_) => Value::Buffer(Vec::from(arg.as_bytes())),
			Ok(i) => Value::Integer(i),
		}
	}
}

fn new_command(cmd: &String, args: &[String]) -> Command {
	Command {
		command: cmd.clone(),
		arguments: VecDeque::from_iter(
			args.iter().map(arg_to_value)
		)
	}
}

fn value_to_string (v: &Value) -> String {
	match v {
		Value::Nill => String::from("nill"),
		Value::Ok => String::from("OK"),
		Value::Bool(b) => format!("{}", b),
		Value::Integer(i) => format!("{}", i),
		Value::Float(n) => format!("{}", f64::from_bits(*n)),
		Value::Buffer(b) => {
			match std::str::from_utf8(&b[..]) {
				Ok(s) => format!("{:?}", s),
				Err(_) => format!("{:?}", &b[..]),
			}
		},
		Value::Array(a) => format!("[{}]", a.iter().map(value_to_string).collect::<Vec<String>>().join(", ")),
		Value::Error(e) => e.clone(),
	}
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn request(sock: &mut TcpStream, cmd: Command) -> Result<Value> {
	let buf = rmp_serde::to_vec(&cmd)?;
	println!("{:?}", buf);
	let len = u16::try_from(buf.len())?;
	sock.write_u16(len).await?;
	sock.write_all(&buf[..]).await?;

	let len = sock.read_u16().await?;
	let mut buf = vec![0; len as usize];
	sock.read_exact(&mut buf[..]).await?;
	println!("{:?}", buf);

	Ok(rmp_serde::from_read_ref(&buf)?)
}

#[tokio::main]
async fn main() -> Result<()> {
	let addr = "127.0.0.1:6142";
	let args: Vec<String> = std::env::args().collect();

	let mut sock = TcpStream::connect(addr).await?;

	if args.len() > 1 {
		let cmd = new_command(&args[1], &args[2..]);
		let result = request(&mut sock, cmd).await?;
		println!("{}", value_to_string(&result));
	} else {
		let mut lines = BufReader::new(tokio::io::stdin()).lines();
		while let Ok(Some(line)) = lines.next_line().await {
			let args: Vec<String> = line.split(" ").map(|i|i.trim().to_owned()).filter(|s|!s.is_empty()).collect();
			let cmd = new_command(&args[0], &args[1..]);
			let result = request(&mut sock, cmd).await?;
			println!("{}", value_to_string(&result));
		}
	}

	Ok(())
}


