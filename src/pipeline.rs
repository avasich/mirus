use chrono::{DateTime, Utc};

use crate::{
    event::Callback,
    measure::{ConnectionConfig, Measure},
    mirror::{Mirror, Protocol},
};

#[derive(Default, Debug)]
pub struct Pipeline {
    operations: Vec<Op>,
}

impl Pipeline {
    #[must_use]
    pub const fn new() -> Self {
        Self { operations: Vec::new() }
    }

    #[must_use]
    pub fn with_operation(mut self, operation: Op) -> Self {
        self.operations.push(operation);
        self
    }

    #[must_use]
    pub fn sort(self, key: SortKey, dir: SortDir) -> Self {
        self.with_operation(Op::sort(key, dir))
    }

    #[must_use]
    pub fn take(self, count: usize) -> Self {
        self.with_operation(Op::take(count))
    }

    #[must_use]
    pub fn filter(self, criteria: FilterCriteria) -> Self {
        self.with_operation(Op::filter(criteria))
    }

    /// add download rate measurement to the pipeline. no-op if measurement already scheduled
    #[must_use]
    pub fn measure_rate(self, config: ConnectionConfig) -> Self {
        if self.operations.iter().all(|op| !matches!(op, Op::MeasureRate(_))) {
            self.with_operation(Op::measure_rate(config)).filter(FilterCriteria::RateMeasured)
        } else {
            self
        }
    }

    #[must_use]
    pub fn with_operations(mut self, operations: Vec<Op>) -> Self {
        self.operations = operations;
        self
    }

    pub async fn execute<'m>(&self, mirrors: &'m [Mirror], notify: impl Callback) -> Vec<&'m Mirror> {
        let mut mirrors: Vec<&Mirror> = mirrors.iter().collect();

        for op in &self.operations {
            match op {
                Op::Sort { criteria } => mirrors.sort_by(|lhs, rhs| criteria.compare(lhs, rhs)),
                Op::Take { count } => mirrors.truncate(*count),
                Op::Filter { criteria } => mirrors.retain(|mirror| criteria.matches(mirror)),
                Op::MeasureRate(config) => Measure::new(*config).batch(&mirrors, notify.clone()).await,
            }
        }
        mirrors
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    CompPct,
    Country,
    Delay,
    Duration,
    IpV4,
    IpV6,
    LastSync,
    Protocol,
    Score,
    Rate,
    Url,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortCriteria {
    pub key: SortKey,
    pub dir: SortDir,
}


impl SortCriteria {
    #[must_use]
    pub const fn new(field: SortKey, dir: SortDir) -> Self {
        Self { key: field, dir }
    }

    #[must_use]
    pub fn compare(self, lhs: &Mirror, rhs: &Mirror) -> std::cmp::Ordering {
        let ord = match self.key {
            SortKey::CompPct => f64::total_cmp(&lhs.completion_pct, &rhs.completion_pct),
            SortKey::Country => Ord::cmp(&lhs.country, &rhs.country),
            SortKey::Delay => Ord::cmp(&lhs.delay, &rhs.delay),
            SortKey::Duration => f64::total_cmp(&lhs.duration_avg, &rhs.duration_avg),
            SortKey::IpV4 => Ord::cmp(&lhs.ipv4, &rhs.ipv4),
            SortKey::IpV6 => Ord::cmp(&lhs.ipv6, &rhs.ipv6),
            SortKey::LastSync => Ord::cmp(&lhs.last_sync, &rhs.last_sync),
            SortKey::Protocol => Ord::cmp(&lhs.protocol, &rhs.protocol),
            SortKey::Score => f64::total_cmp(&lhs.score, &rhs.score),
            SortKey::Rate => Ord::cmp(&*lhs.rate.read().unwrap(), &*rhs.rate.read().unwrap()),
            SortKey::Url => Ord::cmp(&lhs.url, &rhs.url),
        };
        if self.dir == SortDir::Desc { ord.reverse() } else { ord }
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cmp {
    Lt,
    Le,
    Eq,
    Gt,
    Ge,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpV {
    V4,
    V6,
}


#[derive(Debug, Clone, PartialEq)]
pub enum FilterCriteria {
    CompPct { cmp: Cmp, target: f64 },
    Country { allowed: Vec<String> },
    Delay { cmp: Cmp, target: u32 },
    Duration { cmp: Cmp, target: f64 },
    IpV { v4: bool, v6: bool },
    LastSync { cmp: Cmp, target: DateTime<Utc> },
    Protocol { allowed: Vec<Protocol> },
    Score { cmp: Cmp, target: f64 },
    RateMeasured,
}


impl FilterCriteria {
    #[must_use]
    pub fn matches(&self, mirror: &Mirror) -> bool {
        fn compare<T: PartialOrd>(lhs: &T, rhs: &T, cmp: Cmp) -> bool {
            use std::cmp::Ordering;

            match lhs.partial_cmp(rhs) {
                Some(Ordering::Less) => matches!(cmp, Cmp::Lt | Cmp::Le),
                Some(Ordering::Equal) => matches!(cmp, Cmp::Le | Cmp::Eq | Cmp::Ge),
                Some(Ordering::Greater) => matches!(cmp, Cmp::Ge | Cmp::Gt),
                None => false,
            }
        }

        match self {
            Self::CompPct { cmp, target } => compare(&mirror.completion_pct, target, *cmp),
            Self::Country { allowed } => allowed.iter().any(|country| {
                mirror.country.eq_ignore_ascii_case(country) || mirror.country_code.eq_ignore_ascii_case(country)
            }),
            Self::Delay { cmp, target } => compare(&mirror.delay, target, *cmp),
            Self::Duration { cmp, target } => compare(&mirror.duration_avg, target, *cmp),
            Self::IpV { v4, v6 } => (!v4 || mirror.ipv4) && (!v6 || mirror.ipv6),
            Self::LastSync { cmp, target } => compare(&mirror.last_sync, target, *cmp),
            Self::Protocol { allowed } => allowed.contains(&mirror.protocol),
            Self::Score { cmp, target } => compare(&mirror.score, target, *cmp),
            Self::RateMeasured => mirror.rate.read().unwrap().is_some(),
        }
    }

    #[must_use]
    pub const fn completion_pct(cmp: Cmp, target: f64) -> Self {
        Self::CompPct { cmp, target }
    }

    #[must_use]
    pub const fn delay(cmp: Cmp, target: u32) -> Self {
        Self::Delay { cmp, target }
    }

    #[must_use]
    pub const fn duration(cmp: Cmp, target: f64) -> Self {
        Self::Duration { cmp, target }
    }

    #[must_use]
    pub const fn score(cmp: Cmp, target: f64) -> Self {
        Self::Score { cmp, target }
    }

    #[must_use]
    pub const fn ip_version(v4: bool, v6: bool) -> Self {
        Self::IpV { v4, v6 }
    }

    #[must_use]
    pub const fn protocol(allowed: Vec<Protocol>) -> Self {
        Self::Protocol { allowed }
    }

    #[must_use]
    pub const fn country(allowed: Vec<String>) -> Self {
        Self::Country { allowed }
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Sort { criteria: SortCriteria },
    Take { count: usize },
    Filter { criteria: FilterCriteria },
    MeasureRate(ConnectionConfig),
}


impl Op {
    #[must_use]
    pub const fn sort(field: SortKey, dir: SortDir) -> Self {
        Self::Sort {
            criteria: SortCriteria::new(field, dir),
        }
    }

    #[must_use]
    pub const fn take(count: usize) -> Self {
        Self::Take { count }
    }

    #[must_use]
    pub const fn filter(criteria: FilterCriteria) -> Self {
        Self::Filter { criteria }
    }

    #[must_use]
    pub const fn measure_rate(config: ConnectionConfig) -> Self {
        Self::MeasureRate(config)
    }
}
