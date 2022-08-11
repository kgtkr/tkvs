use bytes::Bytes;
use derive_more::{From, Into};
use once_cell::sync::Lazy;
use std::ops;

// 任意の区間を半開区間で表現するためのクエリで使える値

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RangeElement {
    Value(Bytes),
    NegInfSuffix(Bytes),
    Inf,
}

impl RangeElement {
    pub fn min() -> Self {
        RangeElement::Value(Bytes::new())
    }

    pub fn max() -> Self {
        RangeElement::Inf
    }
}

impl Ord for RangeElement {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn to_vec(v: &RangeElement) -> Vec<Option<u8>> {
            match v {
                RangeElement::Value(v) => v.iter().map(|b| Some(*b)).collect(),
                RangeElement::NegInfSuffix(v) => {
                    let mut v = v.iter().map(|b| Some(*b)).collect::<Vec<_>>();
                    v.push(None);
                    v
                }
                RangeElement::Inf => unreachable!(),
            }
        }

        match (self, other) {
            (RangeElement::Inf, RangeElement::Inf) => std::cmp::Ordering::Equal,
            (RangeElement::Inf, _) => std::cmp::Ordering::Greater,
            (_, RangeElement::Inf) => std::cmp::Ordering::Less,
            (a, b) => {
                let a = to_vec(a);
                let b = to_vec(b);
                a.cmp(&b)
            }
        }
    }
}

impl PartialOrd for RangeElement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Bytes> for RangeElement {
    fn from(v: Bytes) -> Self {
        RangeElement::Value(v)
    }
}

static EMPTY_BYTES: Lazy<Bytes> = Lazy::new(Bytes::new);

#[derive(Debug, Clone, Into, From)]
pub struct BytesRange(ops::Range<RangeElement>);

impl BytesRange {
    pub fn from_bounds<R: ops::RangeBounds<Bytes>>(range: R) -> Self {
        let start = match range.start_bound() {
            ops::Bound::Included(v) => RangeElement::Value(v.clone()),
            ops::Bound::Excluded(v) => RangeElement::NegInfSuffix(v.clone()),
            ops::Bound::Unbounded => RangeElement::min(),
        };
        let end = match range.end_bound() {
            ops::Bound::Included(v) => RangeElement::NegInfSuffix(v.clone()),
            ops::Bound::Excluded(v) => RangeElement::Value(v.clone()),
            ops::Bound::Unbounded => RangeElement::max(),
        };
        BytesRange(start..end)
    }
}

impl ops::RangeBounds<Bytes> for BytesRange {
    fn start_bound(&self) -> ops::Bound<&Bytes> {
        match &self.0.start {
            RangeElement::Value(v) => ops::Bound::Included(v),
            RangeElement::NegInfSuffix(v) => ops::Bound::Excluded(v),
            RangeElement::Inf => ops::Bound::Included(&*EMPTY_BYTES),
        }
    }

    fn end_bound(&self) -> ops::Bound<&Bytes> {
        if let RangeElement::Inf = self.0.start {
            ops::Bound::Excluded(&*EMPTY_BYTES)
        } else {
            match &self.0.end {
                RangeElement::Value(v) => ops::Bound::Excluded(v),
                RangeElement::NegInfSuffix(v) => ops::Bound::Included(v),
                RangeElement::Inf => ops::Bound::Unbounded,
            }
        }
    }
}
