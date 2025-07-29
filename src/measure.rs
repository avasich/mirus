use std::{
    borrow::Borrow,
    time::{Duration, Instant},
};

use futures::stream::StreamExt;

use crate::{
    event::{Callback, Event, MeasureEvent},
    mirror::{Mirror, Protocol, Rate},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub connection_timeout: Duration,
    pub timeout: Duration,
    pub max_concurrent_measurements: usize,
    pub keep_timeouted: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(2),
            timeout: Duration::from_secs(5),
            max_concurrent_measurements: 12,
            keep_timeouted: true,
        }
    }
}

impl ConnectionConfig {
    #[must_use]
    pub const fn new(
        connection_timeout: Duration,
        timeout: Duration,
        max_concurrent_measurements: usize,
        keep_timeouted: bool,
    ) -> Self {
        Self {
            connection_timeout,
            timeout,
            max_concurrent_measurements,
            keep_timeouted,
        }
    }

    #[must_use]
    pub const fn with_max_concurrent_measurements(mut self, max_concurrent_measurements: usize) -> Self {
        self.max_concurrent_measurements = max_concurrent_measurements;
        self
    }

    #[must_use]
    pub const fn with_connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.connection_timeout = connection_timeout;
        self
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub const fn keep_timeouted(mut self, keep: bool) -> Self {
        self.keep_timeouted = keep;
        self
    }
}


#[derive(Debug, Clone)]
pub struct Measure {
    config: ConnectionConfig,
}

impl Measure {
    #[must_use]
    pub const fn new(config: ConnectionConfig) -> Self {
        Self { config }
    }

    async fn single_http(
        &self,
        mirror: &Mirror,
        mut progress: impl FnMut(MeasureEvent),
    ) -> Result<Rate, crate::error::NetworkError> {
        progress(MeasureEvent::Connecting);
        let client = reqwest::Client::builder()
            .user_agent("mirror-probe")
            .timeout(self.config.timeout)
            .connect_timeout(self.config.connection_timeout)
            .build()?;

        let connection_start = Instant::now();
        let response = client
            .get(mirror.db_url())
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .await?
            .error_for_status()?;
        let connection_time = connection_start.elapsed();
        progress(MeasureEvent::Connected {
            connection_time,
            file_size: response.content_length(),
        });

        let mut bytes_stream = response.bytes_stream();
        let mut rate = Rate {
            connection_time,
            download_time: Duration::ZERO,
            bytes_downloaded: 0,
        };

        let download_start = Instant::now();
        while let Some(bytes) = bytes_stream.next().await {
            match bytes {
                Ok(bytes) => {
                    rate.bytes_downloaded += bytes.len() as u64;
                    rate.download_time = download_start.elapsed();
                    progress(MeasureEvent::BytesReceived { rate });
                },
                Err(e) if e.is_timeout() && self.config.keep_timeouted => break,
                Err(e) => return Err(e.into()),
            }
        }
        rate.download_time = download_start.elapsed();
        progress(MeasureEvent::Finished { rate });
        Ok(rate)
    }

    // TODO
    #[allow(clippy::unused_async, unused_variables)]
    async fn single_rsync(
        &self,
        mirror: &Mirror,
        mut progress: impl FnMut(MeasureEvent),
    ) -> Result<Rate, crate::error::NetworkError> {
        progress(MeasureEvent::Connecting);
        let size = 1000;
        let chunk = 10;
        let t = Duration::from_millis(40);
        progress(MeasureEvent::Connected {
            connection_time: Duration::ZERO,
            file_size: Some(size),
        });
        let mut rate = Rate {
            connection_time: Duration::ZERO,
            download_time: Duration::ZERO,
            bytes_downloaded: 0,
        };
        let download_start = Instant::now();
        for i in (0..size).step_by(chunk) {
            tokio::time::sleep(t).await;
            rate.download_time = download_start.elapsed();
            rate.bytes_downloaded = i;
            progress(MeasureEvent::BytesReceived { rate });
        }
        rate.download_time = download_start.elapsed();

        progress(MeasureEvent::Finished { rate });
        Ok(rate)
    }

    pub async fn single(&self, mirror: &Mirror, mut progress: impl FnMut(MeasureEvent)) -> Result<Rate, crate::Error> {
        match mirror.protocol {
            Protocol::Http | Protocol::Https => self.single_http(mirror, &mut progress).await,
            Protocol::Rsync => self.single_rsync(mirror, &mut progress).await,
        }
        .inspect_err(|e| progress(MeasureEvent::Failed(e.clone())))
        .map_err(crate::Error::from)
    }

    pub async fn batch<'m, M>(&self, mirrors: impl IntoIterator<Item = &'m M>, notify: impl Callback)
    where
        M: Borrow<Mirror> + Send + Sync + 'm,
    {
        futures::stream::iter(mirrors.into_iter().map(move |m| (m.borrow(), notify.clone())))
            .for_each_concurrent(Some(self.config.max_concurrent_measurements), async move |(mirror, mut cb)| {
                let rate = self.single(mirror, move |event| cb(Event::Measure { id: mirror.id, event })).await;
                let _ = mirror.rate.set(rate.ok());
            })
            .await;
    }
}
