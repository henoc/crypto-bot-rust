use labo::export::anyhow::Result;
use lettre::{SmtpTransport, transport::smtp::authentication::Credentials, Transport, Message};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::credentials::CREDENTIALS;


pub static MAILER: Lazy<Mutex<SmtpTransport>> = Lazy::new(|| {
    let mailer = SmtpTransport::relay("smtp.gmail.com")
        .unwrap()
        .credentials(Credentials::new(CREDENTIALS.mail.user.clone(), CREDENTIALS.mail.password.clone()))
        .build();
    Mutex::new(mailer)
});

pub fn send_mail(subject: String, body: String) -> Result<()> {
    MAILER.lock().send(
        &Message::builder()
            .from(CREDENTIALS.mail.user.parse()?)
            .to(CREDENTIALS.mail.sendto.parse()?)
            .subject(subject)
            .body(body)?
    )?;
    Ok(())
}

#[test]
fn test_send_mail() {
    send_mail("test".to_string(), "test".to_string()).unwrap();
}