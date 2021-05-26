//! Type used on peer store
use crate::peer_store::{Score, SessionType, ADDR_MAX_FAILURES, ADDR_MAX_RETRIES, ADDR_TIMEOUT_MS};
use ipnetwork::IpNetwork;
use p2p::multiaddr::{Multiaddr, Protocol};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Peer info
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Address
    pub connected_addr: Multiaddr,
    /// Session type
    pub session_type: SessionType,
    /// Connected time
    pub last_connected_at_ms: u64,
}

impl PeerInfo {
    /// Init
    pub fn new(
        connected_addr: Multiaddr,
        session_type: SessionType,
        last_connected_at_ms: u64,
    ) -> Self {
        PeerInfo {
            connected_addr,
            session_type,
            last_connected_at_ms,
        }
    }
}

/// Address info
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct AddrInfo {
    /// Multiaddr
    pub addr: Multiaddr,
    /// Score about this addr
    pub score: Score,
    /// Last connected time
    pub last_connected_at_ms: u64,
    /// Last try time
    pub last_tried_at_ms: u64,
    /// Attempts count
    pub attempts_count: u32,
    /// Random id
    pub random_id_pos: usize,
}

impl AddrInfo {
    /// Init
    pub fn new(addr: Multiaddr, last_connected_at_ms: u64, score: Score) -> Self {
        AddrInfo {
            addr,
            score,
            last_connected_at_ms,
            last_tried_at_ms: 0,
            attempts_count: 0,
            random_id_pos: 0,
        }
    }

    /// Whether already connected
    pub fn had_connected(&self, expires_ms: u64) -> bool {
        self.last_connected_at_ms > expires_ms
    }

    /// Whether already try dail within a minute
    pub fn tried_in_last_minute(&self, now_ms: u64) -> bool {
        self.last_tried_at_ms >= now_ms.saturating_sub(60_000)
    }

    /// Whether terrible peer
    pub fn is_terrible(&self, now_ms: u64) -> bool {
        // do not remove addr tried in last minute
        if self.tried_in_last_minute(now_ms) {
            return false;
        }
        // we give up if never connect to this addr
        if self.last_connected_at_ms == 0 && self.attempts_count >= ADDR_MAX_RETRIES {
            return true;
        }
        // consider addr is terrible if failed too many times
        if now_ms.saturating_sub(self.last_connected_at_ms) > ADDR_TIMEOUT_MS
            && (self.attempts_count >= ADDR_MAX_FAILURES)
        {
            return true;
        }
        false
    }

    /// Try dail count
    pub fn mark_tried(&mut self, tried_at_ms: u64) {
        self.last_tried_at_ms = tried_at_ms;
        self.attempts_count = self.attempts_count.saturating_add(1);
    }

    /// Mark last connected time
    pub fn mark_connected(&mut self, connected_at_ms: u64) {
        self.last_connected_at_ms = connected_at_ms;
        // reset attempts
        self.attempts_count = 0;
    }
}

/// Banned addr info
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BannedAddr {
    /// Ip address
    pub address: IpNetwork,
    /// Ban until time
    pub ban_until: u64,
    /// Ban reason
    pub ban_reason: String,
    /// Ban time
    pub created_at: u64,
}

/// Convert multiaddr to IpNetwork
pub fn multiaddr_to_ip_network(multiaddr: &Multiaddr) -> Option<IpNetwork> {
    for addr_component in multiaddr {
        match addr_component {
            Protocol::IP4(ipv4) => return Some(IpNetwork::V4(ipv4.into())),
            Protocol::IP6(ipv6) => return Some(IpNetwork::V6(ipv6.into())),
            _ => (),
        }
    }
    None
}

/// Convert IpAddr to IpNetwork
pub fn ip_to_network(ip: IpAddr) -> IpNetwork {
    match ip {
        IpAddr::V4(ipv4) => IpNetwork::V4(ipv4.into()),
        IpAddr::V6(ipv6) => IpNetwork::V6(ipv6.into()),
    }
}
