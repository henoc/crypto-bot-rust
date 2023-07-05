use anyhow::anyhow;
use futures::{stream::SplitSink, SinkExt};
use log::info;
use tap::Pipe;
use tokio::{spawn, net::TcpStream};
use tokio_tungstenite::{WebSocketStream, MaybeTlsStream, tungstenite::Message};

use crate::{symbol::Symbol, client::mail::send_mail, error_types::BotError};

/// anyhow::Resultのエラーを回収してメールを送信したのちunwrapして再起動させる
pub fn capture_result(symbol: &Symbol) -> impl Fn(anyhow::Result<()>) + '_ {
    let l =  |result: anyhow::Result<()>| {
        match &result {
            Ok(_) => (),
            Err(e) if matches!(e.downcast_ref::<BotError>(), Some(BotError::Maintenance)) => info!("Maintenance status found"),
            Err(e) => {
                send_mail(format!("{} - {} {}", e, symbol.exc, symbol.to_native()), format!("{:?}", e)).unwrap();
                result.unwrap()
            },
        }
    };
    l
}

/// aiohttpのheartbeat相当。こちらからpingを送信する。pong確認で接続検査が必要かも
pub async fn start_send_ping(symbol: Symbol, mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) {
    spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            write.send(Message::Ping(vec![])).await.map_err(|e| anyhow!(e)).pipe(capture_result(&symbol));
        }
    });
}