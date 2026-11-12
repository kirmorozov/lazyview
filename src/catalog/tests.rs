use super::*;

#[test]
fn unique_keys_and_generated_dependencies() {
    let mut cols = vec![
        Column::simple("id", "int"),
        Column::simple("code", "varchar"),
        Column::simple("email", "varchar"),
        Column::simple("a", "int"),
        Column::simple("b", "int"),
    ];
    for c in &mut cols {
        c.nullable = c.name == "email";
    }
    let row = |i: &str, n: u64, c: Option<&str>, sub: Option<u64>| {
        (i.to_string(), n, c.map(str::to_string), sub, String::new())
    };
    let rows = vec![
        row("u_ab", 1, Some("a"), None),
        row("u_ab", 2, Some("b"), None),
        row("u_code", 1, Some("code"), None),
        row("u_email", 1, Some("email"), None),
        row("u_func", 1, None, None),
        row("u_pref", 1, Some("code"), Some(3)),
        row("u_pref", 2, Some("a"), None),
    ];
    assert_eq!(unique_keys_from(&rows, &cols), vec![vec![3, 4], vec![1]]);
    let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
    assert_eq!(generated_deps("(`a` + `b`)", &names), Some(vec![3, 4]));
    assert_eq!(generated_deps("`a` * 2", &names), Some(vec![3]));
    assert_eq!(
        generated_deps("concat(`code`,_utf8mb4'x')", &names),
        Some(vec![1])
    );
    assert_eq!(generated_deps("((", &names), None);
    // MySQL / Percona print the expression string-escaped (B3; measured on 8.4).
    let mysql = r#"concat(`code`,_latin1\'it\\\'s\',_latin1\'\\\\x\',_latin1\'q"d\')"#;
    assert_eq!(
        unescape_generation_expression(mysql),
        r#"concat(`code`,_latin1'it\'s',_latin1'\\x',_latin1'q"d')"#
    );
    assert_eq!(
        generated_deps(&unescape_generation_expression(mysql), &names),
        Some(vec![1])
    );
    assert_eq!(
        generated_deps(
            &unescape_generation_expression(r"json_unquote(json_extract(`a`,_utf8mb4\'$.a\'))"),
            &names
        ),
        Some(vec![3])
    );
    let mut g = Column::simple("g", "int");
    g.generated = Some(Generated {
        expr: "a".into(),
        stored: false,
        deps: Some(vec![3]),
    });
    assert!(!g.exact_key());
    g.generated.as_mut().unwrap().stored = true;
    assert!(g.exact_key());
}

#[test]
fn labels() {
    assert_eq!(
        parse_labels("enum('a','b''c','d,e')"),
        vec!["a", "b'c", "d,e"]
    );
    assert_eq!(col_kind("int", "int(10) unsigned"), ColKind::Unsigned(32));
    assert_eq!(col_kind("INT", "int"), ColKind::Plain);
    assert_eq!(col_kind("timestamp", "timestamp(6)"), ColKind::Timestamp);
    assert_eq!(col_kind("bit", "bit(12)"), ColKind::Bit(12));
    assert_eq!(col_kind("bit", "bit"), ColKind::Bit(1));
}

#[test]
fn params_and_families() {
    let b = Column::new("b", "binary", "binary(4)", None, None, Some(4), false);
    assert_eq!(b.param, "CAST(? AS BINARY(4))");
    let vb = Column::new("b", "varbinary", "varbinary(4)", None, None, Some(4), false);
    assert_eq!(vb.param, "CAST(? AS BINARY)");
    let bit = Column::new("f", "bit", "bit(3)", None, None, None, false);
    assert_eq!(bit.param, "CAST(? AS UNSIGNED)");
    assert_eq!(bit.family(), TypeFamily::Bit);
    let a = Column::new(
        "a",
        "varchar",
        "varchar(5)",
        Some("utf8mb4"),
        Some("utf8mb4_bin"),
        Some(20),
        false,
    );
    let c = Column::new(
        "c",
        "varchar",
        "varchar(5)",
        Some("utf8mb4"),
        Some("utf8mb4_general_ci"),
        Some(20),
        false,
    );
    assert_ne!(
        a.family(),
        c.family(),
        "different collations are different families"
    );
    let i = Column::simple("i", "int");
    let u = Column::new("u", "bigint", "bigint unsigned", None, None, None, false);
    assert_eq!(i.family(), u.family(), "signedness does not matter");
    assert_ne!(i.family(), a.family());
}

#[test]
fn opaque_types_and_default_collations() {
    for (dt, ct) in [
        ("uuid", "uuid"),
        ("inet4", "inet4"),
        ("inet6", "inet6"),
        ("geometry", "geometry"),
        ("point", "point"),
        ("vector", "vector(3)"),
    ] {
        let c = Column::new("c", dt, ct, None, None, Some(16), true);
        assert_eq!(c.kind, ColKind::Opaque, "{dt}");
        assert!(!c.exact_key(), "{dt}");
        assert_eq!(c.param, "?");
    }
    for dt in ["int", "varchar", "decimal", "date", "datetime", "time"] {
        assert!(Column::simple("c", dt).exact_key(), "{dt}");
    }
    let s = Column::new(
        "s",
        "varchar",
        "varchar(5)",
        Some("utf8mb4"),
        Some("utf8mb4_0900_ai_ci"),
        Some(20),
        false,
    );
    assert_eq!(
        s.param,
        "CONVERT(CAST(? AS BINARY) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci"
    );
    assert_eq!(
        s.clone()
            .with_default_collation(Some("utf8mb4_0900_ai_ci"))
            .param,
        "CONVERT(CAST(? AS BINARY) USING utf8mb4)"
    );
    assert_eq!(
        s.clone()
            .with_default_collation(Some("utf8mb4_general_ci"))
            .param,
        s.param,
        "a non-default collation stays explicit"
    );
    assert_eq!(s.clone().with_default_collation(None).param, s.param);
    let i = Column::simple("i", "int");
    assert_eq!(i.clone().with_default_collation(Some("x")).param, i.param);
    let e = Column::new(
        "e",
        "enum",
        "enum('a')",
        Some("utf8mb4"),
        Some("utf8mb4_general_ci"),
        Some(4),
        false,
    );
    assert_eq!(
        e.with_default_collation(Some("utf8mb4_general_ci")).param,
        "?"
    );
}
