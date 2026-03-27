use x509_parser::prelude::*;

/// Extract the Common Name (CN) from a DER-encoded X.509 certificate.
pub fn extract_cn_from_der(der: &[u8]) -> Result<String, String> {
    let (_, cert) = X509Certificate::from_der(der)
        .map_err(|e| format!("Failed to parse X.509 certificate: {e}"))?;
    let subject = cert.subject();
    for rdn in subject.iter() {
        for attr in rdn.iter() {
            if attr.attr_type() == &oid_registry::OID_X509_COMMON_NAME {
                let cn = attr
                    .attr_value()
                    .as_str()
                    .map_err(|e| format!("CN value is not a valid string: {e}"))?;
                return Ok(cn.to_string());
            }
        }
    }
    Err("Certificate has no CN in subject".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_cert_der(cn: &str) -> Vec<u8> {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.distinguished_name.push(
            rcgen::DnType::CommonName,
            rcgen::DnValue::Utf8String(cn.to_string()),
        );
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        cert.der().to_vec()
    }

    #[test]
    fn extract_cn_pg_cert() {
        let der = generate_cert_der("kalamdb-pg-myapp");
        let cn = extract_cn_from_der(&der).expect("extract CN");
        assert_eq!(cn, "kalamdb-pg-myapp");
    }

    #[test]
    fn extract_cn_node_cert() {
        let der = generate_cert_der("kalamdb-node-1");
        let cn = extract_cn_from_der(&der).expect("extract CN");
        assert_eq!(cn, "kalamdb-node-1");
    }

    #[test]
    fn extract_cn_invalid_der() {
        let result = extract_cn_from_der(&[0x00, 0x01, 0x02]);
        assert!(result.is_err());
    }

    #[test]
    fn extract_cn_empty_subject() {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let result = extract_cn_from_der(cert.der());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no CN"));
    }
}
