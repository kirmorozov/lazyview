use super::*;
use crate::access::Hop;
use crate::catalog::{FkAction, ForeignKey};
use crate::dedup::testkit::{catalog, catalog_fk, plan, plan_in, row, row_typed, table, Cell, Rec};

fn table_ref(name: &str) -> TableRef {
    ("db".into(), name.into())
}

/// Compiles `sql` as target 0.
struct C {
    c: Consumer,
    hops: Hops,
}

fn compile(sql: &str) -> (C, AccessPlan) {
    let p = plan(sql);
    let ap = AccessPlan::of(&p);
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
    (C { c, hops }, ap)
}

/// Compiles every `(target, sql)` into one consumer, in the given order.
fn compile_all(plans: &[(usize, &str)]) -> (C, Vec<AccessPlan>) {
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    let aps = plans
        .iter()
        .map(|(t, sql)| {
            let p = plan(sql);
            let ap = AccessPlan::of(&p);
            let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
            c.add(*t, 0, &p, &ap, &spaces, &mut hops).unwrap();
            ap
        })
        .collect();
    (C { c, hops }, aps)
}

/// Compiles `sql` with occurrence `ai`'s hop rewritten to the given plan data: the way to
/// exercise a composite link and a multi-column partner, which the analyzer's chain walks
/// give only through two id aliases (access.rs `chain` tests the shape directly).
fn compile_hop(
    sql: &str,
    ai: usize,
    link_cols: &[usize],
    partner_cols: &[(usize, usize)],
) -> (C, AccessPlan) {
    let p = plan(sql);
    let mut ap = AccessPlan::of(&p);
    let (positions, aliases, select) = match &ap.occurrences[ai].path {
        Path::Hop(h) => (h.positions.clone(), h.aliases.clone(), h.select.clone()),
        other => panic!("occurrence {ai} is not a hop: {other:?}"),
    };
    ap.occurrences[ai].path = Path::Hop(Hop {
        link_cols: link_cols.to_vec(),
        partner: vec![],
        partner_cols: partner_cols.to_vec(),
        positions,
        aliases,
        select,
    });
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
    (C { c, hops }, ap)
}

/// The compiled path of the occurrence the definition aliases `alias`.
fn path<'a>(ap: &'a AccessPlan, alias: &str) -> &'a Path {
    &ap.occurrences
        .iter()
        .find(|o| o.alias == alias)
        .unwrap_or_else(|| panic!("no occurrence aliased `{alias}`"))
        .path
}

const DIRECT: &str = "SELECT i.id, i.kind_id FROM items i";
const FILTERED: &str = "SELECT i.id, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
                        AND v.key_id = 99 AND v.scope_id = 0";

#[test]
fn direct_ids_from_both_images() {
    let (mut x, _) = compile(DIRECT);
    let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
    let mut rec = Rec::default();
    // Insert [1, 7, 0].
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(7), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(1, Op::Insert)]);
    // Update to [1, 8, 0]: it keeps its id, so it is marked once.
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(7), Some(0)])),
        Some(&row(&[Some(1), Some(8), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![(1, Op::Insert), (1, Op::Update)]
    );
    // Update [1, 8, 0] to [2, 8, 0]: the old id goes, the new one comes.
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(8), Some(0)])),
        Some(&row(&[Some(2), Some(8), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    // Delete [2, 8, 0].
    x.c.row(
        &s,
        Some(&row(&[Some(2), Some(8), Some(0)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![
            (1, Op::Insert),
            (1, Op::Update),
            (1, Op::Delete),
            (2, Op::Insert),
            (2, Op::Delete),
        ]
    );
}

#[test]
fn unread_column_update_marks_nothing() {
    let (mut x, _) = compile(FILTERED);
    let s =
        x.c.source(&table_ref("item_values"), Some(5))
            .expect("watched");
    let mut rec = Rec::default();
    x.c.row(
        &s,
        Some(&row(&[Some(4), Some(99), Some(0), Some(5), Some(1)])),
        Some(&row(&[Some(4), Some(99), Some(0), Some(5), Some(2)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, true)], "only `note` changed");
    assert!(rec.ids.is_empty(), "{rec:?}");
}

#[test]
fn filters_drop_other_rows_and_rows_leaving_them() {
    let (mut x, _) = compile(FILTERED);
    let s =
        x.c.source(&table_ref("item_values"), Some(5))
            .expect("watched");
    let mut rec = Rec::default();
    // A row the filters never let in.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, true)], "key_id 98 fails the filters");
    assert!(rec.ids.is_empty(), "{rec:?}");
    // A row leaving them: its id is gone.
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
        Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(5, Op::Delete)]);
    // A short image is not a full image: it passes the filters.
    x.c.row(&s, None, Some(&row(&[Some(5)])), &mut x.hops, &mut rec);
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![(5, Op::Delete), (5, Op::Insert)]
    );
    assert_eq!(rec.rows, vec![(0, true), (0, false), (0, false)]);
}

#[test]
fn a_row_comes_back_to_the_filters_and_another_leaves_them() {
    // The two rows that cross the occurrence's filters: only the image that passes them is
    // the occurrence's row, so the side that fails them contributes no id - the arriving row
    // has no before-image left to compare with, so it is new to the occurrence, not kept.
    let (mut x, _) = compile(FILTERED);
    let s =
        x.c.source(&table_ref("item_values"), Some(5))
            .expect("watched");
    let mut rec = Rec::default();
    // Re-entry: key_id 98 (not the filters' row) becomes key_id 99 (it is).
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
        Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, false)], "the after-image is the row");
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![(5, Op::Insert)],
        "the id is new to the occurrence, not kept: {rec:?}"
    );
    // The reverse: key_id 99 becomes 98, so only the before-image is the row and its id is gone.
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
        Some(&row(&[Some(5), Some(98), Some(0), Some(1), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![(5, Op::Insert), (5, Op::Delete)],
        "{rec:?}"
    );
    assert_eq!(rec.rows, vec![(0, false), (0, false)]);
}

#[test]
fn partial_ids_go_to_their_space() {
    let p = plan(
        "SELECT l.parent_id, i.id, MAX(v.val) AS v FROM item_links l JOIN items i ON i.id \
         = l.child_id JOIN item_values v ON v.item_id = i.id AND v.key_id = 7 GROUP BY \
         l.parent_id, i.id",
    );
    let ap = AccessPlan::of(&p);
    let set = match &ap.occurrences[2].path {
        Path::Partial { set, .. } => *set,
        other => panic!("item_values is not a partial path: {other:?}"),
    };
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
    let s = c
        .source(&table_ref("item_values"), Some(5))
        .expect("watched");
    let mut rec = Rec::default();
    c.row(
        &s,
        None,
        Some(&row(&[Some(4), Some(7), Some(0), Some(9), Some(0)])),
        &mut hops,
        &mut rec,
    );
    assert_eq!(rec.ints(0, SpaceId::partial(set)), vec![(4, Op::Insert)]);
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
}

/// The composite id of one table plus a column that table also reads.
const TUPLE_AND_A_COLUMN: &str = "SELECT v.item_id, v.key_id, MAX(v.val) AS m FROM \
                                   item_values v GROUP BY v.item_id, v.key_id";

#[test]
fn a_tuple_id_that_keeps_its_parts_is_marked_once() {
    // The occurrence's id is two of its table's columns and it reads a third: an UPDATE of
    // that column alone is the tuple path's `same` branch, which no other test reaches (the
    // composite fixture below changes an id column, so its `same` is never true).
    let (mut x, ap) = compile(TUPLE_AND_A_COLUMN);
    assert!(
        matches!(&ap.occurrences[0].path, Path::Direct { cols } if cols == &[0, 1]),
        "{:?}",
        ap.occurrences[0].path
    );
    let s =
        x.c.source(&table_ref("item_values"), Some(5))
            .expect("watched");
    let mut rec = Rec::default();
    // Insert [5, 99, 0, 1, 0]: a new tuple.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    // Update `val` only: both id columns stay, so the tuple is marked once, as an update.
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(99), Some(0), Some(1), Some(0)])),
        Some(&row(&[Some(5), Some(99), Some(0), Some(2), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    let id = vec![Datum::Int(5), Datum::Int(99)];
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![(id.clone(), Op::Insert), (id.clone(), Op::Update)],
        "an id that stays is not a delete plus an insert: {rec:?}"
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    // An update that does move an id column still marks the old tuple gone and the new one in.
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(99), Some(0), Some(2), Some(0)])),
        Some(&row(&[Some(6), Some(99), Some(0), Some(2), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![
            (id.clone(), Op::Insert),
            (id.clone(), Op::Update),
            (id, Op::Delete),
            (vec![Datum::Int(6), Datum::Int(99)], Op::Insert),
        ],
        "{rec:?}"
    );
}

#[test]
fn composite_ids_are_tuples() {
    // The view id is two columns: every id reaches the sink's tuple hook.
    let (mut x, _) = compile("SELECT l.child_id, l.parent_id FROM item_links l");
    let s =
        x.c.source(&table_ref("item_links"), Some(2))
            .expect("watched");
    let mut rec = Rec::default();
    // Insert [3, 4].
    x.c.row(
        &s,
        None,
        Some(&row_typed(&[Cell::Int(3), Cell::Int(4)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![(vec![Datum::Int(3), Datum::Int(4)], Op::Insert)]
    );
    // Update [3, 4] to [3, 5]: the old tuple goes, the new one comes.
    x.c.row(
        &s,
        Some(&row_typed(&[Cell::Int(3), Cell::Int(4)])),
        Some(&row_typed(&[Cell::Int(3), Cell::Int(5)])),
        &mut x.hops,
        &mut rec,
    );
    // Delete [3, 5].
    x.c.row(
        &s,
        Some(&row_typed(&[Cell::Int(3), Cell::Int(5)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![
            (vec![Datum::Int(3), Datum::Int(4)], Op::Insert),
            (vec![Datum::Int(3), Datum::Int(4)], Op::Delete),
            (vec![Datum::Int(3), Datum::Int(5)], Op::Insert),
            (vec![Datum::Int(3), Datum::Int(5)], Op::Delete),
        ]
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
}

#[test]
fn a_string_id_is_a_tuple() {
    // One id column, not an integer: still a tuple, never an int.
    let (mut x, _) = compile("SELECT s.slug, s.kind_id FROM slugs s");
    let s = x.c.source(&table_ref("slugs"), Some(2)).expect("watched");
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row_typed(&[Cell::Text("abc"), Cell::Int(7)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![(vec![Datum::Bytes(b"abc".to_vec())], Op::Insert)]
    );
}

#[test]
fn an_unsigned_id_above_i64_max_is_a_tuple() {
    // The split is on the decoded Datum: an UNSIGNED BIGINT above i64::MAX decodes to
    // Datum::UInt, so the sink's tuple hook fires; one that fits an i64 stays the int hook.
    const OVER: u64 = 1 << 63;
    let (mut x, _) = compile("SELECT c.id, c.kind_id FROM counters c");
    let s =
        x.c.source(&table_ref("counters"), Some(2))
            .expect("watched");
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row_typed(&[Cell::UInt(OVER), Cell::Int(7)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![(vec![Datum::UInt(OVER)], Op::Insert)]
    );
    x.c.row(
        &s,
        None,
        Some(&row_typed(&[Cell::UInt(5), Cell::Int(7)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(5, Op::Insert)]);
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![(vec![Datum::UInt(OVER)], Op::Insert)],
        "the above-i64::MAX id is still the only tuple"
    );
}

#[test]
fn a_smallint_id_stays_the_int_hook() {
    // A narrower signed type is still one Datum::Int: the int hook, with its value. A
    // negative value catches a decode that mistook it for unsigned (65533 then).
    let (mut x, _) = compile("SELECT t.code, t.kind_id FROM tags t");
    let s = x.c.source(&table_ref("tags"), Some(2)).expect("watched");
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row_typed(&[Cell::Int(-3), Cell::Int(7)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(-3, Op::Insert)]);
    assert!(rec.tuples(0, SpaceId::FULL).is_empty(), "{rec:?}");
}

#[test]
fn shape_mismatch_skips_the_occurrence() {
    let (mut x, _) = compile(DIRECT);
    let s = x.c.source(&table_ref("items"), Some(4)).expect("watched");
    assert_eq!(x.c.mismatched(&s), vec![0]);
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(7), Some(0), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
    // Any shape reaches it.
    let s = x.c.source(&table_ref("items"), None).expect("watched");
    assert!(x.c.mismatched(&s).is_empty());
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(7), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(1, Op::Insert)]);
}

#[test]
fn closed_targets_get_nothing() {
    let (mut x, _) = compile(DIRECT);
    let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
    let mut rec = Rec::default();
    rec.closed.push(0);
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(7), Some(0)])),
        Some(&row(&[Some(1), Some(8), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
}

#[test]
fn remove_drops_a_target() {
    let (mut x, _) = compile(DIRECT);
    x.c.remove(0);
    assert!(!x.c.watched(&table_ref("items")), "no occurrence is left");
    let mut rec = Rec::default();
    if let Some(s) = x.c.source(&table_ref("items"), Some(3)) {
        x.c.row(
            &s,
            None,
            Some(&row(&[Some(1), Some(7), Some(0)])),
            &mut x.hops,
            &mut rec,
        );
    }
    assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
}

#[test]
fn two_targets_share_a_source_table() {
    const KEY_98: &str = "SELECT i.id, v.val FROM items i JOIN item_values v ON v.item_id = i.id \
                        AND v.key_id = 98 AND v.scope_id = 0";
    // Target 1 first, then 0, then 1 again: its skipped targets come out unsorted with a
    // duplicate.
    let (mut x, _aps) = compile_all(&[(1, FILTERED), (0, DIRECT), (1, KEY_98)]);
    let items = table_ref("items");
    assert!(x.c.watched(&items), "both targets watch items");
    assert!(
        x.c.watched(&table_ref("item_values")),
        "target 1's joined table is watched too"
    );
    // One row change: both targets' occurrences of items see it, in occurrence order.
    let s = x.c.source(&items, Some(3)).expect("watched");
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(7), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(1, false), (0, false), (1, false)]);
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(1, Op::Insert)]);
    assert_eq!(
        rec.ints(1, SpaceId::FULL),
        vec![(1, Op::Insert), (1, Op::Insert)]
    );
    // A TABLE_MAP of another shape: occurrences 1, 0 and 1 are all skipped, and the skip
    // entries come out ascending, once each.
    let s = x.c.source(&items, Some(4)).expect("watched");
    assert_eq!(x.c.mismatched(&s), vec![0, 1]);
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(7), Some(0), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
    // remove(0) keeps every occurrence of target 1.
    x.c.remove(0);
    assert!(x.c.watched(&items), "target 1 still watches items");
    assert!(
        x.c.watched(&table_ref("item_values")),
        "target 1 still watches item_values"
    );
    let s = x.c.source(&items, Some(3)).expect("watched");
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(2), Some(7), Some(0)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(1, false), (1, false)]);
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    assert_eq!(
        rec.ints(1, SpaceId::FULL),
        vec![(2, Op::Insert), (2, Op::Insert)]
    );
    // Only when the last target goes does the table stop being watched.
    x.c.remove(1);
    assert!(!x.c.watched(&items), "no occurrence of items is left");
    assert!(
        !x.c.watched(&table_ref("item_values")),
        "no occurrence of item_values is left"
    );
}

/// Every table `ap` can make the consumer watch: its occurrences and both sides of its cascades.
fn plan_tables(ap: &AccessPlan) -> Vec<TableRef> {
    let mut v: Vec<TableRef> = ap.occurrences.iter().map(|o| o.table.clone()).collect();
    for c in &ap.cascades {
        v.push(c.parent.clone());
        v.push(c.child.clone());
    }
    v.sort();
    v.dedup();
    v
}

/// [`catalog`] with a cascading FK onto `item_values` whose parent columns give every unit
/// value, which is the analyzer's `Via::Direct`.
fn cascading_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
    let mut cat = catalog();
    let mut t = (*cat[&table_ref("item_values")]).clone();
    t.cascades = vec![ForeignKey {
        parent: table_ref("item_links"),
        child_cols: vec![0, 1],
        parent_cols: vec!["child_id".into(), "parent_id".into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    }];
    cat.insert(table_ref("item_values"), Arc::new(t));
    cat
}

/// `add` compiles everything before it inserts anything, so each refusal leaves every table of
/// the plan unwatched. (Hop paths compile since 6J; their tests are the last group below.)
#[test]
fn a_refusal_watches_no_table_of_the_plan() {
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);

    // A route the consumer does not take: the analyzer's Recompute route, over two tables, so
    // an occurrence compiled before the refusal would show.
    let p = plan("SELECT MAX(v.val) AS m FROM items i JOIN item_values v ON v.item_id = i.id");
    let ap = AccessPlan::of(&p);
    assert!(matches!(ap.route, Route::Recompute), "{ap}");
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    assert!(
        c.add(0, 0, &p, &ap, &spaces, &mut hops)
            .unwrap_err()
            .starts_with("route recompute"),
        "the route is the reason"
    );
    for t in plan_tables(&ap) {
        assert!(!c.watched(&t), "the route refusal watched {t:?}");
    }

    // A path the consumer does not take, on a plan whose route is Dedup: the analyzer gives
    // `Whole` only with the Full and Recompute routes, so the second occurrence carries it
    // here. The first one compiled, so a half-watch would leave `items` watched.
    let p = plan(FILTERED);
    let mut ap = AccessPlan::of(&p);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    assert_eq!(ap.occurrences.len(), 2, "items and item_values");
    ap.occurrences[1].path = Path::Whole;
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let err = c.add(1, 0, &p, &ap, &spaces, &mut hops).unwrap_err();
    assert_eq!(err, "`v`: Whole");
    for t in plan_tables(&ap) {
        assert!(!c.watched(&t), "the path refusal watched {t:?}");
    }

    // A cascade the consumer cannot follow even after 6K: `Via::Untraced`, forced onto a
    // plan the route still lets through (its own `Via::Direct` compiles now). Its parent
    // is a table no occurrence reads, so the refusal must not have left the child watched
    // either. (The map refusal is `map_cascades_are_refused_until_6l`.)
    let p = plan_in(TUPLE_AND_A_COLUMN, &cascading_catalog());
    let mut ap = AccessPlan::of(&p);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    assert!(
        matches!(ap.cascades[0].via, Via::Direct { .. }),
        "{:?}",
        ap.cascades
    );
    ap.cascades[0].via = Via::Untraced {
        reason: "no unit".into(),
    };
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let err = c.add(2, 0, &p, &ap, &spaces, &mut hops).unwrap_err();
    assert!(err.contains("untraced"), "{err}");
    for t in plan_tables(&ap) {
        assert!(!c.watched(&t), "the cascade refusal watched {t:?}");
    }
}

/// The same `item_values` twice, giving the two parts of the unit in different columns: `v1`
/// the second part through its `item_id`, `v2` the first through its `key_id`.
const TWO_PARTIALS: &str = "SELECT l.parent_id, i.id, MAX(v1.val) AS a, MAX(v2.val) AS b \
                            FROM item_links l JOIN items i ON i.id = l.child_id JOIN \
                            item_values v1 ON v1.item_id = i.id AND v1.key_id = 7 JOIN \
                            item_values v2 ON v2.key_id = l.parent_id GROUP BY l.parent_id, \
                            i.id";

#[test]
fn two_partial_sets_keep_their_ids_apart() {
    let p = plan(TWO_PARTIALS);
    let ap = AccessPlan::of(&p);
    let partial = |alias: &str| match path(&ap, alias) {
        Path::Partial {
            set,
            positions,
            cols,
        } => (*set, positions.clone(), cols.clone()),
        other => panic!("`{alias}` is not a partial path: {other:?}"),
    };
    // One consumer, two partial occurrences of one table, each under its own unit positions.
    let (set1, pos1, cols1) = partial("v1");
    let (set2, pos2, cols2) = partial("v2");
    assert_eq!((pos1.as_slice(), cols1.as_slice()), (&[1][..], &[0][..]));
    assert_eq!((pos2.as_slice(), cols2.as_slice()), (&[0][..], &[1][..]));
    let (sp1, sp2) = (SpaceId::partial(set1), SpaceId::partial(set2));
    assert_ne!(sp1, sp2, "different positions, different spaces");
    assert_ne!(sp1, SpaceId::FULL);
    assert_ne!(sp2, SpaceId::FULL);

    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(
        0,
        0,
        &p,
        &ap,
        &Spaces::of(p.unit_exprs.len(), &p.partial.sets),
        &mut hops,
    )
    .unwrap();
    let s = c
        .source(&table_ref("item_values"), Some(5))
        .expect("watched");
    // One row change of the shared table: each occurrence projects its own columns, into its
    // own space, and neither sees the other's id.
    let mut rec = Rec::default();
    c.row(
        &s,
        None,
        Some(&row(&[Some(4), Some(7), Some(0), Some(9), Some(0)])),
        &mut hops,
        &mut rec,
    );
    assert_eq!(
        rec.rows,
        vec![(0, false), (0, false)],
        "both occurrences took the row"
    );
    assert_eq!(rec.ints(0, sp1), vec![(4, Op::Insert)], "v1: its item_id");
    assert_eq!(rec.ints(0, sp2), vec![(7, Op::Insert)], "v2: its key_id");
    assert!(
        rec.tuples(0, sp1).is_empty() && rec.tuples(0, sp2).is_empty(),
        "{rec:?}"
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");

    // A target whose spaces hold no partial set: the compile refuses, and it refuses the whole
    // plan, not the occurrence it was on.
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    let err = c
        .add(
            0,
            0,
            &p,
            &ap,
            &Spaces::of(p.unit_exprs.len(), &[]),
            &mut hops,
        )
        .unwrap_err();
    assert!(err.contains("no space for"), "{err}");
    for t in plan_tables(&ap) {
        assert!(!c.watched(&t), "the space refusal watched {t:?}");
    }
}

/// The one-hop view of access.rs `one_hop_reaches_the_full_id`: `l` reaches the full id through
/// the link, so its rows are looked up in the hop cache and `i`'s rows invalidate it.
const ONE_HOP: &str = "SELECT i.id, l.child_id FROM items i JOIN item_links l ON l.parent_id = \
                       i.kind_id";

/// The two-hop view of access.rs `two_hops_through_two_tables`: `o` reaches the id through `l`
/// and `i`, so both of them are its chain and the partner is `l`.`child_id`.
const TWO_HOPS: &str = "SELECT i.id, MAX(o.name) n FROM items i JOIN item_links l ON \
                        l.parent_id = i.kind_id JOIN owners o ON o.id = l.child_id GROUP BY \
                        i.id";

/// A one-part link value, the way the hop cache keys an entry by it.
fn link(v: i64) -> Tuple {
    Box::new([Datum::Int(v)])
}

/// Cached ids, as [`Hops::answer`] takes them.
fn cached(vs: &[i64]) -> Vec<Key> {
    vs.iter().map(|&v| Key::from_datum(Datum::Int(v))).collect()
}

/// The hop of occurrence `alias` of the compiled target.
fn hop_for(x: &C, alias: usize) -> HopId {
    (0..)
        .map(HopId)
        .find(|h| x.hops.plan(*h).is_some_and(|p| p.alias == alias))
        .expect("that occurrence is not a hop")
}

/// A two-part link value, the way the hop cache keys an entry of a composite link.
fn link2(a: i64, b: i64) -> Tuple {
    Box::new([Datum::Int(a), Datum::Int(b)])
}

/// Cached ids given as two-part tuples, the answers of a hop whose target id has two parts.
fn tuple_ids(pairs: &[(i64, i64)]) -> Vec<Key> {
    pairs
        .iter()
        .map(|&(a, b)| Key::from_parts(Box::new([Datum::Int(a), Datum::Int(b)])))
        .collect()
}

/// An answer for link value `l` of hop `h`, the way a job's completion gives it.
fn cache2(x: &mut C, h: HopId, l: Tuple, ids: Vec<Key>) {
    x.hops.taken(h, &l);
    let what = format!("{l:?}");
    assert!(x.hops.answer(h, l, ids), "{what} is not in flight");
}

/// An answer for link value `v` of hop `h`, the way a job's completion gives it.
fn cache(x: &mut C, h: HopId, v: i64, ids: Vec<Key>) {
    cache2(x, h, link(v), ids);
}

#[test]
fn hop_hit_marks_cached_ids() {
    let (mut x, ap) = compile(ONE_HOP);
    assert!(
        matches!(&ap.occurrences[1].path, Path::Hop(_)),
        "{:?}",
        ap.occurrences[1].path
    );
    let h = hop_for(&x, 1);
    let p = x.hops.plan(h).expect("the hop of `l`");
    assert_eq!(
        (p.link_cols.as_slice(), p.chain.as_slice()),
        (&[1][..], &[0][..]),
        "link `l`.`parent_id`, the chain is `i`"
    );
    assert_eq!(
        p.partner,
        Some((0, vec![1])),
        "the partner is `i`.`kind_id`"
    );
    assert_eq!(p.space, SpaceId::FULL, "the hop reaches the full id");
    cache(&mut x, h, 3, cached(&[10, 11]));
    let s =
        x.c.source(&table_ref("item_links"), Some(2))
            .expect("watched");
    let mut rec = Rec::default();
    // Insert [5, 3]: its link value is cached, so its ids are marked and nothing defers.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(5), Some(3)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![(10, Op::Update), (11, Op::Update)],
        "{rec:?}"
    );
    assert_eq!(rec.hops, vec![(0, true)], "one lookup, a hit");
    assert!(rec.items.is_empty(), "a hit defers nothing: {rec:?}");
}

#[test]
fn hop_miss_defers_one_item_per_link_value() {
    let (mut x, _) = compile(ONE_HOP);
    let h = hop_for(&x, 1);
    let s =
        x.c.source(&table_ref("item_links"), Some(2))
            .expect("watched");
    let mut rec = Rec::default();
    // Update [5, 3] to [5, 4]: nothing is cached, so each link value defers its own item.
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(3)])),
        Some(&row(&[Some(5), Some(4)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.items,
        vec![
            (
                0,
                Item::Hop {
                    hop: h,
                    link: link(3)
                }
            ),
            (
                0,
                Item::Hop {
                    hop: h,
                    link: link(4)
                }
            ),
        ],
        "the old and the new link value, in that order"
    );
    assert_eq!(rec.hops, vec![(0, false), (0, false)], "both missed");
    assert!(rec.ids.is_empty(), "a miss marks nothing: {rec:?}");
}

#[test]
fn delete_through_a_hop_uses_the_before_image() {
    let (mut x, _) = compile(ONE_HOP);
    let h = hop_for(&x, 1);
    cache(&mut x, h, 3, cached(&[10, 11]));
    let s =
        x.c.source(&table_ref("item_links"), Some(2))
            .expect("watched");
    let mut rec = Rec::default();
    // Delete [5, 3]: the before-image is the only one that gives the link value.
    x.c.row(
        &s,
        Some(&row(&[Some(5), Some(3)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.ints(0, SpaceId::FULL),
        vec![(10, Op::Update), (11, Op::Update)],
        "{rec:?}"
    );
    assert_eq!(rec.hops, vec![(0, true)]);
    assert!(rec.items.is_empty(), "{rec:?}");
}

#[test]
fn partner_row_change_evicts_its_values() {
    let (mut x, _) = compile(ONE_HOP);
    let h = hop_for(&x, 1);
    for v in [3, 4, 5] {
        cache(&mut x, h, v, cached(&[100 + v]));
    }
    let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
    let mut rec = Rec::default();
    // An `items` row moves its `kind_id` from 3 to 4: that alias holds every partner column, so
    // both images' partner values lose their entries.
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(3), Some(9)])),
        Some(&row(&[Some(1), Some(4), Some(9)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(x.hops.get(h, &link(3)), None, "the old partner value");
    assert_eq!(x.hops.get(h, &link(4)), None, "the new partner value");
    assert_eq!(
        x.hops.get(h, &link(5)),
        Some(&cached(&[105])[..]),
        "another cached link stays"
    );
    assert!(rec.hops.is_empty(), "`i` is not a hop: {rec:?}");
}

#[test]
fn other_chain_row_clears_the_hop() {
    let (mut x, ap) = compile(TWO_HOPS);
    assert!(
        matches!(&ap.occurrences[2].path, Path::Hop(h) if h.aliases == [0, 1]),
        "{:?}",
        ap.occurrences[2].path
    );
    let oh = hop_for(&x, 2);
    let p = x.hops.plan(oh).expect("the hop of `o`").clone();
    assert_eq!(p.chain, vec![0, 1], "the chain is `i` and `l`");
    assert_eq!(
        p.partner,
        Some((1, vec![0])),
        "the partner is `l`.`child_id`"
    );
    for v in [3, 4] {
        cache(&mut x, oh, v, cached(&[v]));
    }
    let mut rec = Rec::default();
    // An `items` row: a chain alias that is not the partner's, so all of `o`'s hop goes.
    let items = x.c.source(&table_ref("items"), Some(3)).expect("watched");
    x.c.row(
        &items,
        Some(&row(&[Some(1), Some(3), Some(9)])),
        Some(&row(&[Some(1), Some(4), Some(9)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(
        x.hops.is_empty(),
        "a row of another chain alias clears the hop: {p:?}"
    );
    for v in [3, 4, 7] {
        cache(&mut x, oh, v, cached(&[v]));
    }
    // An `item_links` row: the partner's own alias, so only its child_id values go.
    let links =
        x.c.source(&table_ref("item_links"), Some(2))
            .expect("watched");
    x.c.row(
        &links,
        Some(&row(&[Some(3), Some(9)])),
        Some(&row(&[Some(4), Some(9)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(x.hops.get(oh, &link(3)), None, "the old child_id");
    assert_eq!(x.hops.get(oh, &link(4)), None, "the new child_id");
    assert_eq!(
        x.hops.get(oh, &link(7)),
        Some(&cached(&[7])[..]),
        "an entry no partner value of this row names stays"
    );
}

#[test]
fn unread_partner_update_keeps_the_cache() {
    let (mut x, _) = compile(ONE_HOP);
    let h = hop_for(&x, 1);
    cache(&mut x, h, 3, cached(&[10]));
    cache(&mut x, h, 4, cached(&[11]));
    let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
    let mut rec = Rec::default();
    // `owner_id` is no column the view reads: the occurrence does not take the row at all, so
    // the chain invalidation of its alias never runs.
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(3), Some(9)])),
        Some(&row(&[Some(1), Some(3), Some(10)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, true)], "only an unread column changed");
    assert_eq!(x.hops.get(h, &link(3)), Some(&cached(&[10])[..]));
    assert_eq!(x.hops.get(h, &link(4)), Some(&cached(&[11])[..]));
}

/// A hop whose link value is two columns of `l` and whose cached ids are tuples: a hit
/// marks the tuple ids through the `Key::Tuple` branch, a miss defers the whole link.
#[test]
fn composite_link_hit_marks_tuple_ids_and_miss_defers_the_whole_link() {
    // `l`'s (parent_id, child_id) are the link, in that order.
    let (mut x, _) = compile_hop(ONE_HOP, 1, &[1, 0], &[(0, 1), (0, 2)]);
    let h = hop_for(&x, 1);
    let p = x.hops.plan(h).expect("the hop of `l`").clone();
    assert_eq!(
        (p.link_cols.as_slice(), p.chain.as_slice(), p.space),
        (&[1, 0][..], &[0][..], SpaceId::FULL),
        "the link is `l`.`parent_id` then `l`.`child_id`"
    );
    assert_eq!(
        p.partner,
        Some((0, vec![1, 2])),
        "both partner columns are on the one alias `i`: `kind_id` then `owner_id`, \
         matching the link order"
    );
    cache2(&mut x, h, link2(3, 4), tuple_ids(&[(11, 12), (21, 22)]));
    let s =
        x.c.source(&table_ref("item_links"), Some(2))
            .expect("watched");
    let mut rec = Rec::default();
    // Insert child_id 4, parent_id 3: the cached link value marks its tuple ids and
    // defers nothing.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(4), Some(3)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![
            (vec![Datum::Int(11), Datum::Int(12)], Op::Update),
            (vec![Datum::Int(21), Datum::Int(22)], Op::Update),
        ],
        "{rec:?}"
    );
    assert!(rec.items.is_empty(), "a hit defers nothing: {rec:?}");
    // Insert child_id 5, parent_id 3: the link value is not cached, so the item carries
    // the whole two-column value, not just one part of it.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(5), Some(3)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(
        rec.items,
        vec![(
            0,
            Item::Hop {
                hop: h,
                link: link2(3, 5)
            }
        )],
        "the deferred link names both columns: {rec:?}"
    );
    assert_eq!(rec.hops, vec![(0, true), (0, false)], "one hit, one miss");
}

/// The partner of the same two-column hop is `i`'s (kind_id, owner_id) pair: a row of the
/// partner alias evicts both images' partner values, projected together from the image.
#[test]
fn composite_partner_row_evicts_both_values() {
    let (mut x, _) = compile_hop(ONE_HOP, 1, &[1, 0], &[(0, 1), (0, 2)]);
    let h = hop_for(&x, 1);
    for l in [link2(3, 9), link2(4, 10), link2(3, 10)] {
        cache2(&mut x, h, l, cached(&[100]));
    }
    let s = x.c.source(&table_ref("items"), Some(3)).expect("watched");
    let mut rec = Rec::default();
    // `kind_id` 3 to 4 and `owner_id` 9 to 10: both images' partner pairs lose their
    // entries.
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(3), Some(9)])),
        Some(&row(&[Some(1), Some(4), Some(10)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(x.hops.get(h, &link2(3, 9)), None, "the old partner pair");
    assert_eq!(x.hops.get(h, &link2(4, 10)), None, "the new partner pair");
    assert_eq!(
        x.hops.get(h, &link2(3, 10)),
        Some(&cached(&[100])[..]),
        "a pair this row does not name stays: only a whole-hop clear could take it"
    );
    assert!(rec.hops.is_empty(), "`i` is not a hop: {rec:?}");
}

/// Compiles the owner-count view over [`catalog_fk`] as target 0.
fn compile_fk(on_delete: FkAction) -> (C, AccessPlan) {
    let p = plan_in(
        "SELECT i.owner_id, COUNT(*) n FROM items i GROUP BY i.owner_id",
        &catalog_fk(on_delete),
    );
    let ap = AccessPlan::of(&p);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
    (C { c, hops }, ap)
}

#[test]
fn parent_delete_marks_the_fk_ids() {
    let (mut x, ap) = compile_fk(FkAction::Cascade);
    assert!(
        matches!(ap.cascades[0].via, Via::Direct { .. }),
        "STOP if not: {:?}",
        ap.cascades
    );
    let s = x.c.source(&table_ref("owners"), Some(2)).expect("watched");
    let mut rec = Rec::default();
    // A delete of owners [7, ...]: the cascade marks the FK value it took with it.
    x.c.row(
        &s,
        Some(&row(&[Some(7), Some(0)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, false)]);
    assert_eq!(
        rec.ids,
        vec![(
            0,
            SpaceId::FULL,
            Key::Int(intset::from_i64(7)),
            Op::Delete,
            true
        )]
    );
    assert!(rec.items.is_empty() && rec.notes.is_empty(), "{rec:?}");
}

#[test]
fn restrict_and_unchanged_keys_mark_nothing() {
    let (mut x, _) = compile_fk(FkAction::Restrict);
    let s = x.c.source(&table_ref("owners"), Some(2)).expect("watched");
    let mut rec = Rec::default();
    // A delete the engine restricts: the view saw nothing.
    x.c.row(
        &s,
        Some(&row(&[Some(7), Some(0)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
    // An update of `name` only: no `parent_cols` value changed, so no cascade.
    x.c.row(
        &s,
        Some(&row(&[Some(7), Some(1)])),
        Some(&row(&[Some(7), Some(2)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.rows.is_empty() && rec.ids.is_empty(), "{rec:?}");
}

#[test]
fn set_null_marks_the_null_key() {
    let (mut x, ap) = compile_fk(FkAction::SetNull);
    assert!(
        matches!(ap.cascades[0].via, Via::Direct { .. }),
        "{:?}",
        ap.cascades
    );
    let s = x.c.source(&table_ref("owners"), Some(2)).expect("watched");
    let mut rec = Rec::default();
    // A delete sets the children's FK to NULL: the old key and the NULL key are marked.
    x.c.row(
        &s,
        Some(&row(&[Some(7), Some(0)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, false)]);
    assert_eq!(rec.ints(0, SpaceId::FULL), vec![(7, Op::Update)]);
    assert_eq!(
        rec.tuples(0, SpaceId::FULL),
        vec![(vec![Datum::Null], Op::Update)]
    );
    assert!(
        rec.ids.iter().all(|(_, _, _, _, cascaded)| *cascaded),
        "{rec:?}"
    );
}

/// The catalog of `accumulate::derived_cascades_use_the_parent_key`: `attrs` with FKs onto
/// `obj` and `kind`, whose (single-column) keys the view pins.
fn derived_catalog() -> HashMap<TableRef, Arc<TableInfo>> {
    let fk = |parent: &str, child_col: usize, parent_col: &str| ForeignKey {
        parent: ("db".into(), parent.into()),
        child_cols: vec![child_col],
        parent_cols: vec![parent_col.into()],
        on_delete: FkAction::Cascade,
        on_update: FkAction::Cascade,
    };
    let mut cat: HashMap<TableRef, Arc<TableInfo>> = [
        table("kind", &[("kid", "smallint unsigned")], &[0]),
        table("obj", &[("oid", "int unsigned")], &[0]),
        table("link", &[("gid", "int"), ("oid", "int unsigned")], &[0, 1]),
        table(
            "attrs",
            &[
                ("id", "int"),
                ("oid", "int unsigned"),
                ("aid", "smallint unsigned"),
                ("val", "int"),
            ],
            &[0],
        ),
    ]
    .into_iter()
    .collect();
    let (_, attrs) = table(
        "attrs",
        &[
            ("id", "int"),
            ("oid", "int unsigned"),
            ("aid", "smallint unsigned"),
            ("val", "int"),
        ],
        &[0],
    );
    let mut attrs = (*attrs).clone();
    attrs.cascades = vec![fk("obj", 1, "oid"), fk("kind", 2, "kid")];
    cat.insert(table_ref("attrs"), Arc::new(attrs));
    cat
}

#[test]
fn derived_cascades() {
    let p = plan_in(
        "SELECT c.gid, c.oid, MAX(a.val) AS x FROM link c \
         JOIN attrs a ON a.oid = c.oid AND a.aid = 7 GROUP BY c.gid, c.oid",
        &derived_catalog(),
    );
    let ap = AccessPlan::of(&p);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    let ci = |parent: &str| {
        ap.cascades
            .iter()
            .position(|c| c.parent.1 == parent)
            .unwrap()
    };
    let (kind, obj) = (ci("kind"), ci("obj"));
    assert!(
        matches!(ap.cascades[kind].via, Via::Derived { .. }),
        "{:?}",
        ap.cascades
    );
    assert!(
        matches!(ap.cascades[obj].via, Via::Derived { .. }),
        "{:?}",
        ap.cascades
    );
    let set = p.cascades[obj].partial_from_parent[0].0;
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
    let parent = |name: &str| c.source(&table_ref(name), Some(1)).expect("watched");
    let (kind_src, obj_src) = (parent("kind"), parent("obj"));

    // A key no alias reads: nothing to do.
    let mut rec = Rec::default();
    c.row(
        &kind_src,
        Some(&row(&[Some(50)])),
        None,
        &mut hops,
        &mut rec,
    );
    c.row(
        &kind_src,
        Some(&row(&[Some(50)])),
        Some(&row(&[Some(51)])),
        &mut hops,
        &mut rec,
    );
    assert!(
        rec.rows.is_empty() && rec.ids.is_empty() && rec.items.is_empty(),
        "{rec:?}"
    );

    // The pinned key: a presence item for the units its presence column names.
    let mut rec = Rec::default();
    c.row(&kind_src, Some(&row(&[Some(7)])), None, &mut hops, &mut rec);
    assert_eq!(rec.rows, vec![(0, false)]);
    assert_eq!(
        rec.items,
        vec![(
            0,
            Item::Presence {
                index: 0,
                value: Datum::Int(7),
                hold: false,
            }
        )]
    );
    assert!(rec.ids.is_empty() && rec.notes.is_empty(), "{rec:?}");

    // A key update into the pinned value: a hold, not a mark.
    let mut rec = Rec::default();
    c.row(
        &kind_src,
        Some(&row(&[Some(50)])),
        Some(&row(&[Some(7)])),
        &mut hops,
        &mut rec,
    );
    assert_eq!(
        rec.items,
        vec![(
            0,
            Item::Presence {
                index: 0,
                value: Datum::Int(7),
                hold: true,
            }
        )]
    );

    // Out of the pinned value, twice: the consumer sends one item per row (the core
    // dedups them per flush, as `push_once` did here).
    let mut rec = Rec::default();
    c.row(
        &kind_src,
        Some(&row(&[Some(7)])),
        Some(&row(&[Some(51)])),
        &mut hops,
        &mut rec,
    );
    c.row(&kind_src, Some(&row(&[Some(7)])), None, &mut hops, &mut rec);
    assert_eq!(
        rec.items,
        vec![
            (
                0,
                Item::Presence {
                    index: 0,
                    value: Datum::Int(7),
                    hold: false,
                }
            ),
            (
                0,
                Item::Presence {
                    index: 0,
                    value: Datum::Int(7),
                    hold: false,
                }
            ),
        ]
    );
    assert!(rec.notes.is_empty(), "{rec:?}");

    // The partial units from the parent's key: the deleted key, the old and the new key
    // of an update, all as updates of the partial set, all cascaded.
    let mut rec = Rec::default();
    c.row(&obj_src, Some(&row(&[Some(5)])), None, &mut hops, &mut rec);
    c.row(
        &obj_src,
        Some(&row(&[Some(6)])),
        Some(&row(&[Some(9)])),
        &mut hops,
        &mut rec,
    );
    assert_eq!(rec.rows, vec![(0, false), (0, false)]);
    assert_eq!(
        rec.ints(0, SpaceId::partial(set)),
        vec![(5, Op::Update), (6, Op::Update), (9, Op::Update)]
    );
    assert!(rec.ints(0, SpaceId::FULL).is_empty(), "{rec:?}");
    assert!(rec.notes.is_empty(), "{rec:?}");

    // A parent image that is not full cannot be read: a note, nothing else (the core
    // never rebuilds).
    let mut rec = Rec::default();
    c.row(&kind_src, Some(&row(&[])), None, &mut hops, &mut rec);
    assert_eq!(rec.notes, vec![(0, Note::ShortParentImage)]);
    assert!(rec.ids.is_empty() && rec.items.is_empty(), "{rec:?}");
}

/// A catalog of `[(name, columns, pk, cascades)]` over the neutral column types of [`table`].
fn catalog_with(
    entries: &[(&str, &[(&str, &str)], &[usize], Vec<ForeignKey>)],
) -> HashMap<TableRef, Arc<TableInfo>> {
    entries
        .iter()
        .map(|(name, cols, pk, cascades)| {
            let (tr, t) = table(name, cols, pk);
            let mut t = (*t).clone();
            t.cascades = cascades.clone();
            (tr, Arc::new(t))
        })
        .collect()
}

/// `child.child_cols -> parent.parent_cols`, `on_delete` on deletes and `Cascade` on updates.
fn fk(parent: &str, child_cols: &[usize], parent_cols: &[&str], on_delete: FkAction) -> ForeignKey {
    ForeignKey {
        parent: table_ref(parent),
        child_cols: child_cols.to_vec(),
        parent_cols: parent_cols.iter().map(|s| (*s).to_string()).collect(),
        on_delete,
        on_update: FkAction::Cascade,
    }
}

/// A one-part parent key, as the cascade cache holds it.
fn fkv(v: i64) -> Tuple {
    vec![Datum::Int(v)].into()
}

/// Compiles `sql` over `cat` as target 0, plan 0.
fn compile_cat(sql: &str, cat: &HashMap<TableRef, Arc<TableInfo>>) -> (C, AccessPlan) {
    let p = plan_in(sql, cat);
    let ap = AccessPlan::of(&p);
    let spaces = Spaces::of(p.unit_exprs.len(), &p.partial.sets);
    let mut c = Consumer::default();
    let mut hops = Hops::new(1000);
    c.add(0, 0, &p, &ap, &spaces, &mut hops).unwrap();
    (C { c, hops }, ap)
}

/// The cascade index of the route whose child is `child`.
fn cas_to(ap: &AccessPlan, child: &str) -> usize {
    ap.cascades
        .iter()
        .position(|c| c.child.1 == child)
        .unwrap_or_else(|| panic!("no cascade onto {child}"))
}

/// Ported from `accumulate::mapped_cascades_keep_and_replay_child_rows`: the map keeps what
/// the alias's filters keep, a parent row replays the kept children through the consumer's
/// own occurrences of the child table, and the notes stand where the old view's rebuild did.
#[test]
fn mapped_cascades_keep_and_replay_child_rows() {
    let cat = catalog_with(&[
        ("grp", &[("gid", "int")], &[0], vec![]),
        ("obj", &[("oid", "int")], &[0], vec![]),
        (
            "link",
            &[("gid", "int"), ("oid", "int")],
            &[0, 1],
            vec![fk("obj", &[1], &["oid"], FkAction::Cascade)],
        ),
        (
            "attrs",
            &[
                ("id", "bigint"),
                ("oid", "int"),
                ("aid", "smallint"),
                ("val", "int"),
            ],
            &[0],
            vec![fk("obj", &[1], &["oid"], FkAction::Cascade)],
        ),
    ]);
    let sql = "SELECT g.gid, COUNT(*) AS n, MAX(a.val) AS x FROM grp g JOIN link c ON \
               c.gid = g.gid JOIN attrs a ON a.oid = c.oid AND a.aid = 7 GROUP BY g.gid";
    let (mut x, ap) = compile_cat(sql, &cat);
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    let (to_attrs, to_link) = (cas_to(&ap, "attrs"), cas_to(&ap, "link"));
    assert!(
        matches!(ap.cascades[to_attrs].via, Via::Map { .. })
            && matches!(ap.cascades[to_link].via, Via::Map { .. }),
        "{:?}",
        ap.cascades
    );
    let s = x.c.source(&table_ref("attrs"), Some(4)).expect("watched");
    let ls = x.c.source(&table_ref("link"), Some(2)).expect("watched");
    let os = x.c.source(&table_ref("obj"), Some(1)).expect("watched");
    let mut rec = Rec::default();
    let kept_of = |rec: &Rec, fk: i64| {
        rec.caches
            .get(&(0, 0))
            .map(|c| {
                c.under(to_attrs, &fkv(fk))
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };

    // The row an alias's filter keeps is kept; an update of a column no route takes a value
    // from leaves it as it was. A child row change sends no id of its own.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(10), Some(7), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    let kept = kept_of(&rec, 10).into_iter().next().expect("kept");
    assert_eq!(
        (kept.fk.to_vec(), kept.complete),
        (vec![Datum::Int(10)], true)
    );
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(10), Some(7), Some(5)])),
        Some(&row(&[Some(1), Some(10), Some(7), Some(6)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(kept_of(&rec, 10).len(), 1, "an unread column keeps the row");
    // Another key value: the alias's filters drop it, so nothing new is kept.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(2), Some(10), Some(8), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(kept_of(&rec, 10).len(), 1);
    // A new FK: kept under it; out of the filters, and deleted: gone.
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(10), Some(7), Some(5)])),
        Some(&row(&[Some(1), Some(11), Some(7), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(kept_of(&rec, 10).is_empty() && kept_of(&rec, 11).len() == 1);
    x.c.row(
        &s,
        Some(&row(&[Some(1), Some(11), Some(7), Some(5)])),
        Some(&row(&[Some(1), Some(11), Some(9), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(kept_of(&rec, 11).is_empty());
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(3), Some(12), Some(7), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    x.c.row(
        &s,
        Some(&row(&[Some(3), Some(12), Some(7), Some(5)])),
        None,
        &mut x.hops,
        &mut rec,
    );
    assert!(kept_of(&rec, 12).is_empty());
    assert!(rec.ids.is_empty() && rec.notes.is_empty(), "{rec:?}");

    // A parent delete replays the kept children through the consumer's own occurrences of
    // their tables: the link row's id as a cascaded delete, the attrs row reaching its target
    // through its hop, exactly as the logged row of the same key would.
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(10), Some(7), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    x.c.row(
        &ls,
        None,
        Some(&row(&[Some(4), Some(10)])),
        &mut x.hops,
        &mut rec,
    );
    let (n, m, k) = (rec.ids.len(), rec.items.len(), rec.hops.len());
    x.c.row(&os, Some(&row(&[Some(10)])), None, &mut x.hops, &mut rec);
    assert_eq!(
        rec.ids[n..],
        [(
            0,
            SpaceId::FULL,
            Key::Int(intset::from_i64(4)),
            Op::Delete,
            true
        )],
        "{rec:?}"
    );
    assert_eq!(rec.hops[k..], [(0, false)], "the attrs row reaches its hop");
    assert!(
        matches!(&rec.items[m..], [(_, Item::Hop { .. })]),
        "{:?}",
        &rec.items[m..]
    );
    assert_eq!(
        rec.notes
            .iter()
            .filter(|(t, _)| *t == 0)
            .filter(|(_, nt)| matches!(nt, Note::CascadeRows(1)))
            .count(),
        2,
        "{rec:?}"
    );
    assert!(kept_of(&rec, 10).is_empty(), "the replay took the rows");

    // A gap: the note, and nothing else.
    let mut rec = Rec::default();
    let mut gap = cascade::Cache::default();
    gap.gap = true;
    rec.caches.insert((0, 0), gap);
    x.c.row(&os, Some(&row(&[Some(20)])), None, &mut x.hops, &mut rec);
    assert!(rec.ids.is_empty() && rec.items.is_empty(), "{rec:?}");
    assert_eq!(
        rec.notes,
        vec![(0, Note::CascadeGap), (0, Note::CascadeGap)],
        "one note a mapped cascade of the parent"
    );

    // While the cache seeds, a child or parent change is only logged: `Logged` holds the
    // position, and `install` is what yields the ids.
    let mut rec = Rec::default();
    rec.caches.insert((0, 0), cascade::Cache::seeding());
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(1), Some(10), Some(7), Some(5)])),
        &mut x.hops,
        &mut rec,
    );
    let (n, h, m) = (rec.ids.len(), rec.hops.len(), rec.items.len());
    x.c.row(&os, Some(&row(&[Some(10)])), None, &mut x.hops, &mut rec);
    assert!(
        rec.ids[n..].is_empty() && rec.hops[h..].is_empty() && rec.items[m..].is_empty(),
        "while the cache seeds the parent gives nothing: {rec:?}"
    );
    assert!(
        rec.notes.iter().all(|(_, nt)| *nt == Note::Logged) && rec.notes.len() == 3,
        "{rec:?}"
    );
    let log = rec
        .caches
        .get_mut(&(0, 0))
        .unwrap()
        .install(cascade::Cache::default());
    assert_eq!(log.len(), 3);
    let mut rec = Rec::default();
    x.c.install(0, 0, log, &mut x.hops, &mut rec);
    assert_eq!(
        rec.hops,
        [(0, false)],
        "the seeded row replays through its hop"
    );
    assert!(
        matches!(&rec.items[..], [(_, Item::Hop { .. })]),
        "{:?}",
        rec.items
    );

    // A kept row whose image lacked a stored column: a note, and no id.
    let mut rec = Rec::default();
    x.c.row(
        &s,
        None,
        Some(&row(&[Some(9), Some(30)])),
        &mut x.hops,
        &mut rec,
    );
    x.c.row(&os, Some(&row(&[Some(30)])), None, &mut x.hops, &mut rec);
    assert!(rec.ids.is_empty(), "{rec:?}");
    assert!(rec.notes.contains(&(0, Note::IncompleteKeptRow)), "{rec:?}");

    // SET NULL: the kept rows come back with their FK cleared. The link row's own key is the
    // group the view groups by, so it goes out once as a cascaded Update and the row stays
    // kept under NULL; the attrs row reaches its hop with both link values.
    let cleared = catalog_with(&[
        ("grp", &[("gid", "int")], &[0], vec![]),
        ("obj", &[("oid", "int")], &[0], vec![]),
        (
            "link",
            &[("gid", "int"), ("oid", "int")],
            &[0, 1],
            vec![fk("obj", &[1], &["oid"], FkAction::SetNull)],
        ),
        (
            "attrs",
            &[
                ("id", "bigint"),
                ("oid", "int"),
                ("aid", "smallint"),
                ("val", "int"),
            ],
            &[0],
            vec![fk("obj", &[1], &["oid"], FkAction::SetNull)],
        ),
    ]);
    let (mut z, _) = compile_cat(sql, &cleared);
    let zs = z.c.source(&table_ref("attrs"), Some(4)).expect("watched");
    let zl = z.c.source(&table_ref("link"), Some(2)).expect("watched");
    let zo = z.c.source(&table_ref("obj"), Some(1)).expect("watched");
    let mut rec = Rec::default();
    z.c.row(
        &zl,
        None,
        Some(&row(&[Some(4), Some(10)])),
        &mut z.hops,
        &mut rec,
    );
    z.c.row(
        &zs,
        None,
        Some(&row(&[Some(1), Some(10), Some(7), Some(5)])),
        &mut z.hops,
        &mut rec,
    );
    let (n, m, h) = (rec.ids.len(), rec.items.len(), rec.hops.len());
    z.c.row(&zo, Some(&row(&[Some(10)])), None, &mut z.hops, &mut rec);
    assert_eq!(
        rec.ids[n..],
        [(
            0,
            SpaceId::FULL,
            Key::Int(intset::from_i64(4)),
            Op::Update,
            true
        )],
        "a child key the clearing leaves alone is one Update: {rec:?}"
    );
    assert!(
        !rec.items[m..].is_empty()
            && rec.items[m..]
                .iter()
                .all(|(_, it)| matches!(it, Item::Hop { .. }))
            && rec.hops[h..].iter().all(|&(_, hit)| !hit),
        "the attrs row reaches its hop: {:?} {:?}",
        &rec.items[m..],
        &rec.hops[h..]
    );
    let null = vec![Datum::Null].into();
    assert_eq!(
        rec.caches[&(0, 0)].under(to_link, &null).len(),
        0,
        "a row whose FK the engine cleared is not kept"
    );
    assert_eq!(
        rec.notes
            .iter()
            .filter(|(_, nt)| matches!(nt, Note::CascadeRows(1)))
            .count(),
        2,
        "both children come back, one a route: {rec:?}"
    );
}

/// Ported from `accumulate::cache_only_cascade_changes_hold_no_replay_position`: the rows of
/// a level the view does not read live only in the cache, and their parent's change reaches
/// the view through the rows kept below them.
#[test]
fn cache_only_cascade_changes_hold_no_replay_position() {
    let levels = |on_delete: FkAction| {
        catalog_with(&[
            ("grp", &[("id", "int")], &[0], vec![]),
            (
                "mid",
                &[("id", "int"), ("gid", "int")],
                &[0],
                vec![fk("grp", &[1], &["id"], on_delete)],
            ),
            (
                "leaf",
                &[("mid_id", "int"), ("seq", "int"), ("qty", "int")],
                &[0, 1],
                vec![fk("mid", &[0], &["id"], on_delete)],
            ),
        ])
    };
    let sql = "SELECT mid_id, seq, qty FROM leaf WHERE qty >= 0";
    let (mut x, ap) = compile_cat(sql, &levels(FkAction::Cascade));
    assert!(matches!(ap.route, Route::Dedup), "{ap}");
    let (to_mid, to_leaf) = (cas_to(&ap, "mid"), cas_to(&ap, "leaf"));
    assert!(
        matches!(ap.cascades[to_mid].via, Via::Map { .. })
            && matches!(ap.cascades[to_leaf].via, Via::Map { .. }),
        "{:?}",
        ap.cascades
    );
    let gs = x.c.source(&table_ref("grp"), Some(1)).expect("watched");
    let ms = x.c.source(&table_ref("mid"), Some(2)).expect("watched");
    let ls = x.c.source(&table_ref("leaf"), Some(3)).expect("watched");
    let kept = |rec: &Rec, ci: usize, fk: i64| {
        rec.caches
            .get(&(0, 0))
            .map(|c| c.under(ci, &fkv(fk)).len())
            .unwrap_or_default()
    };
    let kept_at = |rec: &Rec, ci: usize, fk: Tuple| {
        rec.caches
            .get(&(0, 0))
            .map(|c| c.under(ci, &fk).len())
            .unwrap_or_default()
    };

    // A new row of the level the view does not read: kept, and nothing goes out.
    let mut rec = Rec::default();
    x.c.row(
        &ms,
        None,
        Some(&row(&[Some(5), Some(1)])),
        &mut x.hops,
        &mut rec,
    );
    assert_eq!(kept(&rec, to_mid, 1), 1);
    assert!(
        rec.ids.is_empty() && rec.notes.is_empty() && rec.items.is_empty(),
        "{rec:?}"
    );

    // Its parent deleted: the kept row replays through the consumer's routes of `mid`, of
    // which the view has none, and no leaf row is kept under it: no id.
    let n = rec.notes.len();
    x.c.row(&gs, Some(&row(&[Some(1)])), None, &mut x.hops, &mut rec);
    assert_eq!(rec.notes[n..], [(0, Note::CascadeRows(1))], "{rec:?}");
    assert!(rec.ids.is_empty(), "{rec:?}");
    assert_eq!(kept(&rec, to_mid, 1), 0, "the replay took the row");

    // With a leaf row kept under the next level's id, the same parent change marks that
    // leaf row: one cascaded delete of its key.
    let mut rec = Rec::default();
    x.c.row(
        &ms,
        None,
        Some(&row(&[Some(6), Some(2)])),
        &mut x.hops,
        &mut rec,
    );
    x.c.row(
        &ls,
        None,
        Some(&row(&[Some(6), Some(1), Some(3)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(rec.notes.is_empty(), "{rec:?}");
    let n = rec.ids.len();
    x.c.row(&gs, Some(&row(&[Some(2)])), None, &mut x.hops, &mut rec);
    assert_eq!(
        rec.ids[n..],
        [(
            0,
            SpaceId::FULL,
            Key::Tuple(vec![Datum::Int(6), Datum::Int(1)].into()),
            Op::Delete,
            true
        )],
        "{rec:?}"
    );
    assert_eq!(kept(&rec, to_leaf, 6), 0);

    // SET NULL: the middle row comes back with its own FK cleared, and the view has no route
    // of `mid`: nothing goes out. The leaf below it keeps its rows: their FK points at the
    // middle key, which did not move; a row whose own FK is NULL is not kept at all.
    let (mut y, _) = compile_cat(sql, &levels(FkAction::SetNull));
    let yms = y.c.source(&table_ref("mid"), Some(2)).expect("watched");
    let yls = y.c.source(&table_ref("leaf"), Some(3)).expect("watched");
    let ygs = y.c.source(&table_ref("grp"), Some(1)).expect("watched");
    let mut rec = Rec::default();
    y.c.row(
        &yms,
        None,
        Some(&row(&[Some(6), Some(2)])),
        &mut y.hops,
        &mut rec,
    );
    y.c.row(
        &yls,
        None,
        Some(&row(&[Some(6), Some(1), Some(3)])),
        &mut y.hops,
        &mut rec,
    );
    let (n, m) = (rec.ids.len(), rec.notes.len());
    y.c.row(&ygs, Some(&row(&[Some(2)])), None, &mut y.hops, &mut rec);
    assert!(rec.ids[n..].is_empty(), "{rec:?}");
    assert_eq!(rec.notes[m..], [(0, Note::CascadeRows(1))], "{rec:?}");
    assert_eq!(kept(&rec, to_leaf, 6), 1, "the leaf row stays");
    assert_eq!(
        kept_at(&rec, to_mid, vec![Datum::Null].into()),
        0,
        "a row whose FK the engine cleared is not kept"
    );

    // While the cache seeds, the same change holds the position instead: `Logged`, no id,
    // and `install` yields what the ready cache gave.
    let mut rec = Rec::default();
    rec.caches.insert((0, 0), cascade::Cache::seeding());
    x.c.row(
        &ms,
        None,
        Some(&row(&[Some(7), Some(3)])),
        &mut x.hops,
        &mut rec,
    );
    x.c.row(
        &ls,
        None,
        Some(&row(&[Some(7), Some(1), Some(3)])),
        &mut x.hops,
        &mut rec,
    );
    assert!(
        rec.notes.iter().all(|(_, nt)| *nt == Note::Logged),
        "{rec:?}"
    );
    let (n, m) = (rec.ids.len(), rec.notes.len());
    x.c.row(&gs, Some(&row(&[Some(3)])), None, &mut x.hops, &mut rec);
    assert!(
        rec.ids[n..].is_empty(),
        "the parent change is only logged: {rec:?}"
    );
    assert_eq!(
        rec.notes[m..],
        [(0, Note::Logged)],
        "and it holds the position"
    );
    let log = rec
        .caches
        .get_mut(&(0, 0))
        .unwrap()
        .install(cascade::Cache::default());
    assert_eq!(log.len(), 3, "the two children and the parent");
    let mut rec = Rec::default();
    x.c.install(0, 0, log, &mut x.hops, &mut rec);
    assert_eq!(
        rec.ids,
        [(
            0,
            SpaceId::FULL,
            Key::Tuple(vec![Datum::Int(7), Datum::Int(1)].into()),
            Op::Delete,
            true
        )],
        "{rec:?}"
    );
}
