use bytes::Bytes;

// 任意の区間を半開区間で表現するためのクエリで使える値

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryValue {
    Value(Bytes),
    NegInfSuffix(Bytes),
    Inf,
}

impl QueryValue {
    pub fn min() -> Self {
        QueryValue::Value(Bytes::new())
    }

    pub fn max() -> Self {
        QueryValue::Inf
    }
}

impl Ord for QueryValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn to_vec(v: &QueryValue) -> Vec<Option<u8>> {
            match v {
                QueryValue::Value(v) => v.iter().map(|b| Some(*b)).collect(),
                QueryValue::NegInfSuffix(v) => {
                    let mut v = v.iter().map(|b| Some(*b)).collect::<Vec<_>>();
                    v.push(None);
                    v
                }
                QueryValue::Inf => unreachable!(),
            }
        }

        match (self, other) {
            (QueryValue::Inf, QueryValue::Inf) => std::cmp::Ordering::Equal,
            (QueryValue::Inf, _) => std::cmp::Ordering::Greater,
            (_, QueryValue::Inf) => std::cmp::Ordering::Less,
            (a, b) => {
                let a = to_vec(a);
                let b = to_vec(b);
                a.cmp(&b)
            }
        }
    }
}

impl PartialOrd for QueryValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Bytes> for QueryValue {
    fn from(v: Bytes) -> Self {
        QueryValue::Value(v)
    }
}
