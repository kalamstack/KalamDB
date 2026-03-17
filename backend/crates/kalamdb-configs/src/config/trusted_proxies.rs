use anyhow::anyhow;
use ipnet::IpNet;
use std::net::IpAddr;

/// Parse trusted proxy entries from configuration.
///
/// Supports either CIDR ranges like `10.0.0.0/8` or single IPs like `10.0.1.9`.
pub fn parse_trusted_proxy_entries(entries: &[String]) -> anyhow::Result<Vec<IpNet>> {
    entries
        .iter()
        .filter_map(|entry| {
            let trimmed = entry.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        })
        .map(parse_trusted_proxy_entry)
        .collect()
}

fn parse_trusted_proxy_entry(entry: &str) -> anyhow::Result<IpNet> {
    if let Ok(network) = entry.parse::<IpNet>() {
        return Ok(network);
    }

    let ip = entry
        .parse::<IpAddr>()
        .map_err(|error| anyhow!("invalid trusted proxy entry '{}': {}", entry, error))?;

    Ok(IpNet::from(ip))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    #[test]
    fn parses_single_ip_and_cidr_entries() {
        let parsed = parse_trusted_proxy_entries(&[
            "10.0.1.9".to_string(),
            "192.168.0.0/24".to_string(),
            "2001:db8::/32".to_string(),
        ])
        .unwrap();

        let single_ip: IpAddr = "10.0.1.9".parse().unwrap();
        let cidr_ip: IpAddr = "192.168.0.42".parse().unwrap();
        let ipv6_ip: IpAddr = "2001:db8::1".parse().unwrap();

        assert_eq!(parsed.len(), 3);
        assert!(parsed[0].contains(&single_ip));
        assert!(parsed[1].contains(&cidr_ip));
        assert!(parsed[2].contains(&ipv6_ip));
    }

    #[test]
    fn rejects_invalid_entries() {
        let error = parse_trusted_proxy_entries(&["not-an-ip".to_string()]).unwrap_err();
        assert!(error.to_string().contains("invalid trusted proxy entry 'not-an-ip'"));
    }
}
