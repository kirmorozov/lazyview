use super::*;

pub(super) fn cases() -> Vec<Case> {
    vec![
        // --- merge family: planned exactly like the inline form
        case(
            "cte_filter",
            "incremental",
            "spj",
            "WITH paid AS (SELECT id, customer_id, created_at, status FROM orders WHERE status IN ('paid','shipped'))
             SELECT id, customer_id, created_at FROM paid",
        ),
        case(
            "cte_join",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, name, tier FROM customers WHERE tier IS NOT NULL)
             SELECT o.id, o.status, c.name AS customer, c.tier FROM orders o JOIN c ON c.id = o.customer_id",
        ),
        // The same CTE text in another definition.
        case(
            "cte_join_twin",
            "incremental",
            "grouped",
            "WITH c AS (SELECT id, name, tier FROM customers WHERE tier IS NOT NULL)
             SELECT c.id, c.tier, COUNT(*) AS n FROM c JOIN orders o ON o.customer_id = c.id GROUP BY c.id, c.tier",
        ),
        // Referenced three times, joined to itself through different columns, NULL sides.
        case(
            "cte_multi_ref",
            "incremental",
            "spj",
            "WITH c AS (SELECT id, referrer_id, region_id, name FROM customers)
             SELECT a.id, a.name, b.name AS referrer, b2.name AS referrer2, x.id AS neighbour
             FROM c a JOIN c b ON b.id = a.referrer_id
             LEFT JOIN c b2 ON b2.id = b.referrer_id
             LEFT JOIN c x ON x.region_id = a.region_id AND x.id = a.id + 1",
        ),
        // Column list, unaliased expressions over CTE columns (server-generated names).
        case(
            "cte_column_list",
            "incremental",
            "spj",
            "WITH t (oid, line, amount) AS (SELECT order_id, line_no, qty * price FROM order_items)
             SELECT oid, line, amount, amount * 2, ROUND(amount / 3, 2) FROM t WHERE amount > 50",
        ),
        case(
            "cte_chained",
            "incremental",
            "spj",
            "WITH o AS (SELECT id, customer_id FROM orders WHERE status <> 'cancelled'),
                  oc AS (SELECT o.id, c.name, c.region_id FROM o JOIN customers c ON c.id = o.customer_id)
             SELECT oc.id, oc.name, r.name AS region FROM oc LEFT JOIN regions r ON r.id = oc.region_id",
        ),
        // A WITH inside a derived table inside a CTE.
        case(
            "cte_nested_with",
            "incremental",
            "spj",
            "WITH x AS (SELECT d.oid, d.amount FROM
                          (WITH li AS (SELECT order_id AS oid, line_no, qty * price AS amount FROM order_items)
                           SELECT oid, line_no, amount FROM li WHERE amount > 10) d)
             SELECT oid, amount FROM x",
        ),
        // A CTE named like the table it reads.
        case(
            "cte_shadow",
            "incremental",
            "spj",
            "WITH orders AS (SELECT id, customer_id, status FROM orders WHERE status = 'new')
             SELECT id, customer_id FROM orders",
        ),
        // A2: functions with a value for NULL arguments (CHAR(NULL) = '', CHARSET(NULL) =
        // 'binary', COERCIBILITY(NULL) = 6) on the NULL side: guarded when the derived table
        // or scalar subquery is merged, NULL where the payment is missing.
        case(
            "derived_null_side_char",
            "incremental",
            "spj",
            "SELECT o.id, p.v FROM orders o LEFT JOIN (SELECT order_id, CHAR(65 + (amount > 10)) AS v FROM payments WHERE status = 'ok') p
             ON p.order_id = o.id",
        ),
        case(
            "derived_null_side_charset",
            "incremental",
            "spj",
            "SELECT o.id, p.v, p.c FROM orders o LEFT JOIN (SELECT order_id, CHARSET(method) AS v, COERCIBILITY(method) AS c FROM payments WHERE status = 'ok') p
             ON p.order_id = o.id",
        ),
        case(
            "scalar_null_side_char",
            "incremental",
            "spj",
            "SELECT o.id, (SELECT CHAR(65 + (p.amount > 10)) FROM payments p WHERE p.id = o.id) AS v FROM orders o",
        ),
        // L4: IN / EXISTS / NOT EXISTS / NOT IN over a join (the workload's v_big_spenders):
        // the subquery's tables are traced by direct units where they carry the correlation to
        // the unit, else by lookups (both tables of one subquery together when both changed).
        case(
            "semi_in_join",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c
             WHERE c.id IN (SELECT o.customer_id FROM orders o JOIN payments p ON p.order_id = o.id WHERE p.amount > 400)",
        ),
        case(
            "semi_exists_join_grouped",
            "incremental",
            "grouped",
            "SELECT c.tier, COUNT(*) AS n FROM customers c
             WHERE EXISTS (SELECT 1 FROM orders o JOIN payments p ON p.order_id = o.id
                           WHERE o.customer_id = c.id AND p.status = 'ok' AND p.amount > 200)
             GROUP BY c.tier",
        ),
        case(
            "semi_not_exists_join",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c
             WHERE NOT EXISTS (SELECT 1 FROM orders o JOIN order_items i ON i.order_id = o.id WHERE o.customer_id = c.id AND i.qty > 3)",
        ),
        case(
            "semi_not_in_join",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o
             WHERE o.customer_id NOT IN (SELECT c.id FROM customers c JOIN regions r ON r.id = c.region_id WHERE r.tax_rate > 0.05)",
        ),
        // JSON group keys (MySQL: bound typed by the engine and compared by the stored value;
        // MariaDB: text).
        case(
            "json_group_key",
            "incremental",
            "grouped",
            "SELECT meta, COUNT(*) AS n, MAX(id) AS last FROM customers GROUP BY meta",
        ),
        // Its text (->>) is a plain unit on every flavor; the JSON value a function computes is
        // Full (MariaDB compares it as JSON, a unit filter binds text: groups refreshed empty).
        case(
            "json_text_key",
            "incremental",
            "grouped",
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(meta, '$.lang')) AS lang, COUNT(*) AS n FROM customers
             GROUP BY JSON_UNQUOTE(JSON_EXTRACT(meta, '$.lang'))",
        ),
        case(
            "json_extract_key",
            "full",
            "group key `JSON_EXTRACT(meta, '$.lang')` is a JSON value computed by a function (MariaDB compares it as JSON, its unit filter as text): group by the column, or by its text with ->> / JSON_UNQUOTE",
            "SELECT JSON_EXTRACT(meta, '$.lang') AS lang, COUNT(*) AS n FROM customers GROUP BY JSON_EXTRACT(meta, '$.lang')",
        ),
        // A6: a scalar subquery whose value reads only outer columns is NULL where no row
        // matches (guarded after the LEFT JOIN rewrite), in the projection and in WHERE.
        case(
            "scalar_outer_only",
            "incremental",
            "spj",
            "SELECT o.id, (SELECT o.note FROM payments p WHERE p.id = o.id * 10) AS v,
                    (SELECT CONCAT(o.status, 'x') FROM payments p WHERE p.id = o.id * 10) AS w FROM orders o",
        ),
        case(
            "scalar_outer_only_where",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o WHERE (SELECT o.note FROM payments p WHERE p.id = o.id * 10) IS NULL",
        ),
        // C1: a string literal with a newline before a semi-join (the marker replacement
        // must convert multi-line token locations).
        case(
            "semi_newline_literal",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o WHERE o.note <> 'a\nb'
             AND EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 1)",
        ),
        // A5: a CTE chain whose inlined form would have 64 table references is Full with the
        // budget in the reason (the server evaluates the statement as written).
        case(
            "cte_chain_64",
            "full",
            "inlining the WITH clause reads 64 table references in one SELECT (budget 32): CTE `t5` is referenced 2 times",
            "WITH t0 AS (SELECT id, customer_id, status FROM orders)
             , t1 AS (SELECT a.id, a.customer_id, a.status FROM t0 a JOIN t0 b ON b.id = a.id)
             , t2 AS (SELECT a.id, a.customer_id, a.status FROM t1 a JOIN t1 b ON b.id = a.id)
             , t3 AS (SELECT a.id, a.customer_id, a.status FROM t2 a JOIN t2 b ON b.id = a.id)
             , t4 AS (SELECT a.id, a.customer_id, a.status FROM t3 a JOIN t3 b ON b.id = a.id)
             , t5 AS (SELECT a.id, a.customer_id, a.status FROM t4 a JOIN t4 b ON b.id = a.id)
             , t6 AS (SELECT a.id, a.customer_id, a.status FROM t5 a JOIN t5 b ON b.id = a.id)
             SELECT id, status FROM t6",
        ),
        // A4: `*` over USING / NATURAL joins expanded as the server does (shared columns once,
        // from the preserved side, then the rest): incremental, same columns as the view.
        case(
            "star_using_join",
            "incremental",
            "spj",
            "SELECT * FROM orders JOIN customers USING (id)",
        ),
        case(
            "star_natural_join",
            "incremental",
            "spj",
            "SELECT * FROM customers NATURAL JOIN regions",
        ),
        case(
            "star_right_using",
            "incremental",
            "spj",
            "SELECT * FROM orders RIGHT JOIN customers USING (id)",
        ),
        // Single-table derived table on the NULL side; its constant column is NULL when missing.
        case(
            "derived_null_side",
            "incremental",
            "spj",
            "SELECT o.id, p.method, p.amount, p.has_payment
             FROM orders o LEFT JOIN (SELECT order_id, method, amount, 1 AS has_payment FROM payments WHERE status = 'ok') p
             ON p.order_id = o.id",
        ),
        case(
            "derived_preserved",
            "incremental",
            "spj",
            "SELECT x.id, x.name, r.name AS region
             FROM (SELECT id, name, region_id FROM customers WHERE credit > 100) x
             LEFT JOIN regions r ON r.id = x.region_id",
        ),
        // --- join syntax that only spells an inner / left join differently
        case(
            "join_right",
            "incremental",
            "spj",
            "SELECT o.id, o.status, c.name AS customer FROM customers c RIGHT JOIN orders o ON o.customer_id = c.id",
        ),
        case(
            "join_straight",
            "incremental",
            "spj",
            "SELECT i.order_id, i.line_no, p.sku FROM order_items i STRAIGHT_JOIN products p ON p.id = i.product_id",
        ),
        case(
            "join_nested",
            "incremental",
            "spj",
            "SELECT o.id, i.line_no, c.name FROM (orders o JOIN order_items i ON i.order_id = o.id)
             JOIN customers c ON c.id = o.customer_id",
        ),
        // --- aggregated relations
        // Merged up: group-key predicates go to WHERE, the others to HAVING; NULL group keys.
        case(
            "cte_agg_alone",
            "incremental",
            "grouped",
            "WITH t AS (SELECT o.customer_id, COUNT(*) AS n, SUM(i.qty) AS q
                        FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.customer_id)
             SELECT customer_id, n, q FROM t WHERE n > 1 AND (customer_id IS NULL OR customer_id > 3)",
        ),
        case(
            "cte_agg_global",
            "incremental",
            "global",
            "WITH t AS (SELECT COUNT(*) AS n, SUM(amount) AS s, MAX(paid_at) AS last_paid FROM payments WHERE status = 'ok')
             SELECT n, s, last_paid FROM t",
        ),
        // Joined on the other table's key: the aggregation is pulled up through the join.
        case(
            "cte_agg_key_join",
            "incremental",
            "grouped",
            "WITH t AS (SELECT order_id, SUM(qty * price) AS amount, COUNT(*) AS n FROM order_items GROUP BY order_id)
             SELECT o.id, o.status, t.amount, t.n FROM orders o JOIN t ON t.order_id = o.id WHERE t.n > 1",
        ),
        // ... also through a chain of key joins.
        case(
            "cte_agg_key_chain",
            "incremental",
            "grouped",
            "WITH t AS (SELECT order_id, SUM(qty) AS units FROM order_items GROUP BY order_id)
             SELECT t.order_id, c.name, c.tier, t.units FROM t
             JOIN orders o ON o.id = t.order_id JOIN customers c ON c.id = o.customer_id",
        ),
        // A table without primary key through a CTE, grouped by nullable columns.
        case(
            "cte_events",
            "incremental",
            "grouped",
            "WITH e AS (SELECT customer_id, kind, amount FROM events_log WHERE amount IS NOT NULL)
             SELECT customer_id, kind, COUNT(*) AS n, SUM(amount) AS s FROM e GROUP BY customer_id, kind",
        ),
        // --- DISTINCT = GROUP BY over the output columns
        case(
            "distinct_top",
            "incremental",
            "grouped",
            "SELECT DISTINCT customer_id, status FROM orders",
        ),
        case(
            "distinct_cte",
            "incremental",
            "grouped",
            "WITH t AS (SELECT DISTINCT p.category, i.qty > 5 AS big FROM order_items i JOIN products p ON p.id = i.product_id)
             SELECT category, big FROM t",
        ),
        // NULL-extended rows feeding the DISTINCT tuple: the block planner's NULL-side rule.
        case(
            "distinct_left",
            "full",
            "group key on the NULL side of a LEFT JOIN",
            "SELECT DISTINCT o.status, c.tier FROM orders o LEFT JOIN customers c ON c.id = o.customer_id",
        ),
        // --- semi- and anti-joins
        case(
            "in_unique",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o
             WHERE o.customer_id IN (SELECT id FROM customers WHERE tier IN ('gold','platinum'))",
        ),
        case(
            "exists_on_unit",
            "incremental",
            "spj",
            "SELECT o.id, o.created_at FROM orders o
             WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id AND i.qty > 3)",
        ),
        case(
            "not_exists_on_unit",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o
             WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.status = 'ok')",
        ),
        case(
            "in_on_unit",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders WHERE status = 'paid')",
        ),
        case(
            "cte_in_subquery",
            "incremental",
            "spj",
            "WITH vip AS (SELECT id FROM customers WHERE FIND_IN_SET('vip', flags) > 0)
             SELECT id, customer_id FROM orders WHERE customer_id IN (SELECT id FROM vip)",
        ),
        // Correlated on something else than the unit: the subquery's rows are looked up.
        case(
            "in_lookup",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o WHERE o.customer_id IN (SELECT referrer_id FROM customers WHERE tier = 'gold')",
        ),
        case(
            "not_exists_lookup",
            "incremental",
            "spj",
            "SELECT c.id, c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.referrer_id)",
        ),
        case(
            "grouped_exists",
            "incremental",
            "grouped",
            "SELECT o.customer_id, COUNT(*) AS n FROM orders o
             WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.region_id IS NOT NULL)
             GROUP BY o.customer_id",
        ),
        // Scalar subqueries looking one row up by its key: LEFT JOINs.
        case(
            "scalar_lookup",
            "incremental",
            "spj",
            "SELECT o.id, o.status, (SELECT name FROM customers c WHERE c.id = o.customer_id) AS customer,
                    (SELECT COALESCE(tier, 'none') FROM customers c2 WHERE c2.id = o.customer_id) AS tier
             FROM orders o",
        ),
        case(
            "scalar_unaliased",
            "incremental",
            "spj",
            "SELECT i.order_id, i.line_no, (SELECT sku FROM products p WHERE p.id = i.product_id) FROM order_items i",
        ),
        case(
            "scalar_in_where",
            "incremental",
            "spj",
            "SELECT o.id, o.created_at FROM orders o WHERE (SELECT region_id FROM customers c WHERE c.id = o.customer_id) = 2",
        ),
        // --- block shapes the planner maintains as bounded recomputes / partitions
        case(
            "topn_products",
            "incremental",
            "global",
            "SELECT id, sku, price FROM products WHERE active = 1 ORDER BY price DESC, id LIMIT 5",
        ),
        case(
            "window_partitioned",
            "incremental",
            "grouped",
            "SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS rn,
                    COUNT(*) OVER (PARTITION BY customer_id) AS n
             FROM orders",
        ),
        case(
            "window_whole",
            "incremental",
            "global",
            "SELECT id, RANK() OVER (ORDER BY created_at DESC, id) AS r FROM orders WHERE status = 'paid'",
        ),
        case(
            "keyless_events",
            "incremental",
            "spj",
            "SELECT customer_id, kind, amount FROM events_log WHERE amount > 50",
        ),
        // The group key reduced to what determines it (o.id; c joined on its key).
        case(
            "group_reduced_left",
            "incremental",
            "grouped",
            "SELECT o.id, c.name, COUNT(i.line_no) AS n_lines
             FROM orders o LEFT JOIN customers c ON c.id = o.customer_id LEFT JOIN order_items i ON i.order_id = o.id
             GROUP BY o.id, c.name",
        ),
        case(
            "distinct_reduced",
            "incremental",
            "grouped",
            "SELECT DISTINCT c.id, c.name, c.tier FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = 'paid'",
        ),
        // Anti-join: NULL-extended rows kept by WHERE, the NULL side traced by lookups.
        case(
            "anti_join_is_null",
            "incremental",
            "spj",
            "SELECT o.id, o.status FROM orders o LEFT JOIN payments p ON p.order_id = o.id AND p.status = 'ok' WHERE p.id IS NULL",
        ),
        // --- not maintained incrementally yet (each reason names the construct)
        case(
            "cte_agg_joined",
            "full",
            "GROUP BY in CTE `t` joined to other tables",
            "WITH t AS (SELECT customer_id, SUM(amount) AS total FROM events_log GROUP BY customer_id)
             SELECT c.id, c.name, t.total FROM customers c JOIN t ON t.customer_id = c.region_id",
        ),
        case(
            "cte_union",
            "full",
            "UNION ALL in CTE `u` (needs branch plans)",
            "WITH u AS (SELECT id, 'o' AS src FROM orders WHERE status = 'paid'
                        UNION ALL SELECT id, 'p' FROM payments WHERE status = 'ok')
             SELECT id, src FROM u",
        ),
        case(
            "cte_window",
            "full",
            "window function in CTE `t`",
            "WITH t AS (SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS rn FROM orders)
             SELECT id, customer_id FROM t WHERE rn = 1",
        ),
        case(
            "cte_recursive",
            "full",
            "recursive CTE `r`",
            "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5)
             SELECT o.id, r.n FROM orders o JOIN r ON r.n = o.id % 5 + 1",
        ),
        case(
            "cte_limit",
            "full",
            "ORDER BY … LIMIT in CTE `t`",
            "WITH t AS (SELECT id, price FROM products ORDER BY price DESC, id LIMIT 5) SELECT id, price FROM t",
        ),
        case(
            "not_in",
            "incremental",
            "spj",
            "SELECT id FROM orders WHERE customer_id NOT IN (SELECT referrer_id FROM customers WHERE referrer_id IS NOT NULL)",
        ),
    ]
}
