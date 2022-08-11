use bytes::Bytes;

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
