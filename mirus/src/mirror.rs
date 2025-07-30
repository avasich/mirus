use std::{
    borrow::Borrow,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
    sync::RwLock,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    de::{SeqAccess, Visitor},
};
use url::Url;

use crate::{measure::Callback, pipeline::Pipeline};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Http,
    Https,
    Rsync,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Mirrorlist {
    pub url: Option<Url>,
    pub check_frequency: u32,
    pub cutoff: u32,
    pub last_check: DateTime<Utc>,
    pub num_checks: usize,
    #[serde(rename = "urls", deserialize_with = "stream_mirrors_skip_missing")]
    pub mirrors: Vec<Mirror>,
    pub version: u32,
}

impl Mirrorlist {
    pub const URL: &str = "https://archlinux.org/mirrors/status/json/";

    pub async fn fetch(url: Url) -> Result<Self, crate::Error> {
        let mut mirrorlist = reqwest::get(url.clone()).await?.error_for_status()?.json::<Self>().await?;
        mirrorlist.url = Some(url);
        Ok(mirrorlist)
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, crate::Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mirrorlist = serde_json::from_reader(reader)?;

        Ok(mirrorlist)
    }

    pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), crate::Error> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, self)?;

        Ok(())
    }

    pub async fn apply_pipeline(&self, pipeline: Pipeline, notify: impl Callback) -> Vec<&Mirror> {
        pipeline.execute(&self.mirrors, notify).await
    }

    #[must_use]
    pub fn get(&self, id: MirrorId) -> &Mirror {
        &self.mirrors[id.0]
    }

    pub fn save_formatted<M>(path: impl AsRef<Path>, mirrors: impl IntoIterator<Item = M>) -> Result<(), crate::Error>
    where
        M: Borrow<Mirror>,
    {
        use std::io::Write;

        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        Ok(mirrors
            .into_iter()
            .map(|mirror| mirror.borrow().mirrorlist_entry())
            .try_for_each(|entry| writeln!(&mut writer, "{entry}"))?)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MirrorId(usize);

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Mirror {
    #[serde(skip_deserializing)]
    pub id: MirrorId,
    pub active: bool,
    // The number of mirror checks that have successfully connected and disconnected from the given URL.
    // If this is below 100%, the mirror may be unreliable.
    pub completion_pct: f64,
    pub country: String,
    pub country_code: String,

    // The calculated average mirroring delay; e.g. the mean value of last check −
    // last sync for each check of this mirror URL. Due to the timing of mirror checks,
    // any value under one hour should be viewed as ideal.
    pub delay: u32,
    pub details: Option<String>,

    // The average (mean) time it took to connect and retrieve the lastsync file from the given URL.
    // Note that this connection time is from the location of the Arch server;
    // your geography may product different results.
    pub duration_avg: f64,
    pub duration_stddev: f64,
    pub ipv4: bool,
    pub ipv6: bool,
    pub isos: bool,
    pub last_sync: DateTime<Utc>,
    pub protocol: Protocol,

    // A very rough calculation for ranking mirrors. It is currently calculated as
    // (hours delay + average duration + standard deviation) / completion percentage.
    // Lower is better.
    pub score: f64,
    pub url: Url,

    #[serde(skip_deserializing)]
    pub rate: RwLock<Option<Rate>>,
}


impl Mirror {
    pub const DB_SUBPATH: &str = "extra/os/x86_64/extra.db";

    #[must_use]
    pub fn db_url(&self) -> Url {
        self.url.join(Self::DB_SUBPATH).unwrap()
    }

    pub fn mirrorlist_entry(&self) -> String {
        format!("Server = {}/$repo/os/$arch", self.url)
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Rate {
    pub connection_time: Duration,
    pub download_time: Duration,
    pub bytes_downloaded: u64,
}


impl Rate {
    #[must_use]
    pub fn total_time(&self) -> Duration {
        self.connection_time + self.download_time
    }

    #[must_use]
    pub fn bytes_per_second(&self) -> f64 {
        1000.0 * self.bytes_downloaded as f64 / self.download_time.as_millis() as f64
    }
}

impl Ord for Rate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // b1 / t1 > b2 / t2 => b1 * t2 > b2 * t1
        let x = u128::from(self.bytes_downloaded) * other.download_time.as_millis();
        let y = u128::from(other.bytes_downloaded) * self.download_time.as_millis();
        x.cmp(&y)
    }
}

impl PartialOrd for Rate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Deserialize)]
pub struct RawMirror {
    pub active: bool,
    pub completion_pct: Option<f64>,
    pub country: String,
    pub country_code: String,
    pub delay: Option<u32>,
    pub details: Option<String>,
    pub duration_avg: Option<f64>,
    pub duration_stddev: Option<f64>,

    #[serde(default)]
    pub ipv4: bool,
    #[serde(default)]
    pub ipv6: bool,
    #[serde(default)]
    pub isos: bool,

    pub last_sync: Option<DateTime<Utc>>,
    pub protocol: Protocol,
    pub score: Option<f64>,
    pub url: String,
}


impl TryFrom<RawMirror> for Mirror {
    type Error = crate::Error;

    fn try_from(raw: RawMirror) -> Result<Self, Self::Error> {
        if let Some(completion_pct) = raw.completion_pct
            && let Some(delay) = raw.delay
            && let Some(duration_avg) = raw.duration_avg
            && let Some(duration_stddev) = raw.duration_stddev
            && let Some(score) = raw.score
            && let Some(last_sync) = raw.last_sync
            && raw.active
        {
            Ok(Self {
                id: MirrorId::default(),
                active: true,
                completion_pct,
                country: raw.country,
                country_code: raw.country_code,
                delay,
                details: raw.details,
                duration_avg,
                duration_stddev,
                ipv4: raw.ipv4,
                ipv6: raw.ipv6,
                isos: raw.isos,
                last_sync,
                protocol: raw.protocol,
                score,
                url: Url::parse(&raw.url).map_err(|_| crate::Error::InvalidMirror(String::from("invalid url")))?,
                rate: RwLock::default(),
            })
        } else {
            Err(crate::Error::InvalidMirror(String::from("mirror is not active")))
        }
    }
}


fn stream_mirrors_skip_missing<'de, D>(de: D) -> Result<Vec<Mirror>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StreamVisitor;

    impl<'de> Visitor<'de> for StreamVisitor {
        type Value = Vec<Mirror>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a mirror record with some fields possibly missing or null")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(raw) = seq.next_element::<RawMirror>()? {
                if let Ok(mut mirror) = Mirror::try_from(raw) {
                    mirror.id = MirrorId(out.len());
                    out.push(mirror);
                }
            }
            Ok(out)
        }
    }

    de.deserialize_seq(StreamVisitor)
}
