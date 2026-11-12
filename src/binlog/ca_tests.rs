use super::*;

#[test]
fn ca_params_split_and_applied() {
    let (u, c) = split_ca_params(
        "mysql://u:p@h/d?require_ssl=true&tls_ca=%2Ftmp%2Fa.pem,%2Fb.pem&verify_ca=true",
    );
    assert_eq!(u, "mysql://u:p@h/d?require_ssl=true&verify_ca=true");
    assert_eq!(c, vec!["/tmp/a.pem", "/b.pem"]);
    assert_eq!(split_ca_params("mysql://h/d").1.len(), 0);
    assert!(replica_opts("mysql://h/d?ssl-ca=/definitely/missing.pem").is_err());
    let ca = std::env::current_dir().unwrap().join("Cargo.toml");
    let o = replica_opts(&format!(
        "mysql://h/d?require_ssl=true&tls_ca={}",
        ca.display()
    ))
    .unwrap();
    assert_eq!(o.ssl_opts().unwrap().root_certs().len(), 1);
}
