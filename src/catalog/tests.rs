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
        row("PRIMARY", 1, Some("id"), None),
        row("u_ab", 1, Some("a"), None),
        row("u_ab", 2, Some("b"), None),
        row("u_code", 1, Some("code"), None),
        row("u_email", 1, Some("email"), None),
        row("u_func", 1, None, None),
        row("u_pref", 1, Some("code"), Some(3)),
        row("u_pref", 2, Some("a"), None),
    ];
    let (pk, keys) = keys_from("items", &rows, &cols).unwrap();
    assert_eq!(pk, vec![0]);
    assert_eq!(keys, vec![vec![3, 4], vec![1]]);
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
    assert_eq!(b.param, "CAST(UNHEX(?) AS BINARY(4))");
    let vb = Column::new("b", "varbinary", "varbinary(4)", None, None, Some(4), false);
    assert_eq!(vb.param, "UNHEX(?)");
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
        "CAST(UNHEX(?) AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_0900_ai_ci"
    );
    assert_eq!(
        s.clone()
            .with_default_collation(Some("utf8mb4_0900_ai_ci"))
            .param,
        "CAST(UNHEX(?) AS CHAR CHARACTER SET utf8mb4)"
    );
    assert_eq!(
        s.clone()
            .with_default_collation(Some("utf8mb4_general_ci"))
            .param,
        s.param,
        "a non-default collation stays explicit"
    );
    assert_eq!(s.clone().with_default_collation(None).param, s.param);
    // A non-UTF8 charset: the bytes of the row image are reinterpreted, never converted (MariaDB
    // turned a byte invalid in the connection's utf8mb4 into `?` inside `CONVERT(... USING ...)`),
    // and they arrive as hex (#394, see `byte_images_bind_as_hex`).
    let l = Column::new(
        "l",
        "varchar",
        "varchar(5)",
        Some("latin1"),
        Some("latin1_swedish_ci"),
        Some(5),
        false,
    );
    assert_eq!(
        l.param,
        "CAST(UNHEX(?) AS CHAR CHARACTER SET latin1) COLLATE latin1_swedish_ci"
    );
    assert_eq!(
        l.clone()
            .with_default_collation(Some("latin1_swedish_ci"))
            .param,
        "CAST(UNHEX(?) AS CHAR CHARACTER SET latin1)",
        "the probe's answer drops the collation of a non-UTF8 column too"
    );
    assert!(!l.exact_key(), "latin1 binlog bytes are not unit keys");
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

/// The hex text [`HEX_PARAM`] decodes, decoded the way the server's `UNHEX` does.
fn unhex(t: &[u8]) -> Vec<u8> {
    assert!(t.len().is_multiple_of(2), "odd hex text {t:?}");
    t.chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}

/// #394: the bytes of a string or binary row image reach the server as hex text, and the
/// placeholder decodes them. As a plain string parameter they were utf8mb4 text to the server,
/// and MariaDB sizes such a parameter by its utf8mb4 characters: bytes that only begin a
/// multi-byte character (latin1 `É` = 0xC9, sjis `ﾊ` = 0xCA, a VARBINARY 0xE282) are none, so
/// the lookup's literal row held `''` and the lookup named the wrong unit. Every byte string the
/// rule can bite (each 1- and 2-byte value, each 3-byte start of a 4-byte character, the empty
/// value) binds as ASCII, which every character set reads as itself, and decodes to its bytes.
#[test]
fn byte_images_bind_as_hex() {
    let text = |dt: &str, ct: &str, cs: &str, co: &str| {
        Column::new("c", dt, ct, Some(cs), Some(co), Some(40), false)
    };
    let bytes_cols = [
        text("varchar", "varchar(10)", "latin1", "latin1_swedish_ci"),
        text("char", "char(10)", "latin1", "latin1_bin")
            .with_default_collation(Some("latin1_swedish_ci")),
        text("varchar", "varchar(10)", "sjis", "sjis_japanese_ci")
            .with_default_collation(Some("sjis_japanese_ci")),
        text("text", "text", "utf16", "utf16_general_ci"),
        text("varchar", "varchar(10)", "utf8mb4", "utf8mb4_0900_ai_ci"),
        Column::new("b", "binary", "binary(4)", None, None, Some(4), false),
        Column::new("b", "varbinary", "varbinary(4)", None, None, Some(4), false),
        Column::new("b", "blob", "blob", None, None, None, false),
    ];
    let mut values: Vec<Vec<u8>> = vec![Vec::new()];
    values.extend((0..=255u8).map(|x| vec![x]));
    values.extend((0..=u16::MAX).map(|x| x.to_be_bytes().to_vec()));
    for lead in 0xF0..=0xF4u8 {
        for a in 0x80..=0xBFu8 {
            values.extend((0x80..=0xBFu8).map(|b| vec![lead, a, b]));
        }
    }
    for c in &bytes_cols {
        assert!(
            c.hex && c.param.contains(HEX_PARAM),
            "{}: {}",
            c.column_type,
            c.param
        );
        for v in &values {
            match c.bind(&Datum::Bytes(v.clone())) {
                Value::Bytes(t) => {
                    assert!(t.is_ascii(), "{}: {v:02X?} bound as {t:?}", c.column_type);
                    assert_eq!(&unhex(&t), v, "{}", c.column_type);
                }
                other => panic!("{}: {v:02X?} bound as {other:?}", c.column_type),
            }
        }
        assert_eq!(c.bind(&Datum::Null), Value::NULL, "{}", c.column_type);
    }
    // The other placeholders take the value as it is.
    let others = [
        (Column::simple("i", "int"), Datum::Int(-3)),
        (
            Column::simple("d", "decimal"),
            Datum::Bytes(b"-12.50".to_vec()),
        ),
        (
            Column::simple("t", "time"),
            Datum::Bytes(b"838:00:00.500000".to_vec()),
        ),
        (
            Column::new(
                "e",
                "enum",
                "enum('é')",
                Some("latin1"),
                Some("latin1_swedish_ci"),
                None,
                false,
            ),
            Datum::Bytes("é".as_bytes().to_vec()),
        ),
        (
            Column::new("s", "timestamp", "timestamp(3)", None, None, None, false),
            Datum::Bytes(b"1700000000.125".to_vec()),
        ),
        (
            Column::new("j", "json", "json", None, None, None, true),
            Datum::Bytes(br#"{"a":1}"#.to_vec()),
        ),
    ];
    for (c, d) in &others {
        assert!(
            !c.hex && !c.param.contains(HEX_PARAM),
            "{}: {}",
            c.column_type,
            c.param
        );
        assert_eq!(c.bind(d), d.to_value(), "{}", c.column_type);
    }
}
