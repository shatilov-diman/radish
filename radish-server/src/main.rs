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

use std::convert::TryFrom;
use std::time::{SystemTime, Duration};

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use radish_types::*;
use radish_database::Storage;

async fn command_loop_executor(conn_name: &str, mut sock: TcpStream, mut storage: Storage) -> Result<(), String> {
	loop {
		let len = sock.read_u16().await.map_err(|_|"Failed to read frame size".to_owned())?;
		let mut buf = vec![0; len as usize];
		sock.read_exact(&mut buf[..]).await.map_err(|_|"Failed to read command".to_owned())?;

		let cmd: Command = rmp_serde::from_read_ref(&buf).map_err(|_|"Failed to deserialize command".to_owned())?;
		log::debug!("{}: {}", conn_name, cmd);
		let result = storage.execute(cmd).await;
		log::debug!("{}: {}", conn_name, result);

		let buf = rmp_serde::to_vec(&result).map_err(|_|"Failed to serialize result".to_owned())?;
		let len = u16::try_from(buf.len()).map_err(|_|"Length of result is too big".to_owned())?;
		sock.write_u16(len).await.map_err(|_|"Failed to write frame size".to_owned())?;
		sock.write_all(&buf[..]).await.map_err(|_|"Failed to write result".to_owned())?;
	}
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
	env_logger::init();

	let addr = "127.0.0.1:6142";
	let mut listener = TcpListener::bind(addr).await.unwrap();

	let mut storage = Storage::new();
	let st = storage.clone();
	storage.set_expire_awaker(move |timepoint|{
		let st = st.clone();
		//TODO: each future has low cost but it still take O(N) of memory for each call
		tokio::spawn(async move {
			//1 mill needs because quant size of delay_until is 1ms
			let timepoint = timepoint + Duration::from_millis(1);
			log::debug!("wait untill {:?}", timepoint);
			let delta = loop {
				let now = SystemTime::now();
				let delta = timepoint.duration_since(now).unwrap_or(Duration::new(0, 0));
				if delta <= Duration::from_secs(3700) {
					break delta;
				}
				tokio::time::delay_for(Duration::from_secs(3600)).await;
			};
			tokio::time::delay_for(delta).await;
			st.keys_check_expirations().await;
		});
	});

	loop {
		let (sock, _) = listener.accept().await?;
		let conn_name = format!("{:?}", sock.peer_addr());
		log::info!("{}: connected", conn_name);

		let storage = storage.clone();
		tokio::spawn(async move {
			match command_loop_executor(&conn_name, sock, storage.clone()).await {
				Ok(_) => log::info!("{}: unexpected finished loop", conn_name),
				Err(err) => log::info!("{}: closed with error: {}", conn_name, err),
			}
		});
	}
}

