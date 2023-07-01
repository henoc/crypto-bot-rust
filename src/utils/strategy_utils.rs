use log::info;

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
