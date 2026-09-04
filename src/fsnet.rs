//! Shared upgrade logic for FlowStation's BTS-initiated Telemetry/Control
//! WebSocket channels. Both ride the main Brew/dashboard listener's root `/`
//! route (FlowStation hardcodes that path, non-configurably) and are told
//! apart from each other -- and from a browser loading the dashboard -- by
//! the `Sec-WebSocket-Protocol` the BTS offers: a required echo-or-abort
//! header, unlike Brew's own Digest-challenged handshake.
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        FromRequestParts,
    },
    http::{header, request::Parts, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use base64::Engine;
use std::{collections::HashMap, future::Future};

/// Identity of an authenticated connection: the Basic Auth username, or
/// `None` when the channel has no configured users (auth disabled).
pub type Identity = Option<String>;

/// True if `parts` offered `subprotocol` in `Sec-WebSocket-Protocol`.
pub fn offers_subprotocol(parts: &Parts, subprotocol: &str) -> bool {
    parts
        .headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|offered| offered.split(',').any(|p| p.trim() == subprotocol))
        .unwrap_or(false)
}

/// Verifies optional HTTP Basic auth against `users`, checks the upgrade
/// request, and completes the WebSocket handshake echoing `subprotocol`
/// back (FlowStation aborts if that echo doesn't match exactly). On success,
/// hands the accepted socket and identity to `handler`.
pub async fn upgrade<H, Fut>(
    users: &HashMap<String, String>,
    subprotocol: &'static str,
    parts: &mut Parts,
    handler: H,
) -> Response
where
    H: FnOnce(WebSocket, Identity) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let identity = match verify_basic(users, &parts.headers) {
        Ok(identity) => identity,
        Err(()) => return basic_challenge(subprotocol),
    };

    let unit = ();
    match WebSocketUpgrade::from_request_parts(parts, &unit).await {
        Ok(ws) => ws
            .protocols([subprotocol])
            .on_upgrade(move |socket| handler(socket, identity))
            .into_response(),
        Err(rejection) => rejection.into_response(),
    }
}

fn verify_basic(users: &HashMap<String, String>, headers: &HeaderMap) -> Result<Identity, ()> {
    if users.is_empty() {
        return Ok(None);
    }
    let Some(value) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else { return Err(()) };
    let Some(b64) = value.strip_prefix("Basic ") else { return Err(()) };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64) else { return Err(()) };
    let Ok(text) = String::from_utf8(decoded) else { return Err(()) };
    let Some((user, pass)) = text.split_once(':') else { return Err(()) };
    if users.get(user).map(|p| p == pass).unwrap_or(false) {
        Ok(Some(user.to_string()))
    } else {
        Err(())
    }
}

fn basic_challenge(realm: &str) -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_str(&format!("Basic realm=\"{realm}\"")).unwrap(),
    );
    response
}
