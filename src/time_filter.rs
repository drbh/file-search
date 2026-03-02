use std::path::Path;
use std::time::{Duration, SystemTime};

#[derive(Clone, Copy, Debug, Default)]
pub struct MtimeFilter {
    pub newer_than: Option<Duration>,
    pub older_than: Option<Duration>,
}

impl MtimeFilter {
    #[inline]
    pub fn is_active(&self) -> bool {
        self.newer_than.is_some() || self.older_than.is_some()
    }

    pub fn validate(&self) -> Result<(), String> {
        if let (Some(min_age), Some(max_age)) = (self.older_than, self.newer_than) {
            if min_age > max_age {
                return Err("--min-age cannot be greater than --max-age".to_string());
            }
        }
        Ok(())
    }

    #[inline]
    pub fn matches_modified(&self, modified_at: SystemTime, now: SystemTime) -> bool {
        let age = now.duration_since(modified_at).unwrap_or(Duration::ZERO);

        if let Some(max_age) = self.newer_than {
            if age > max_age {
                return false;
            }
        }

        if let Some(min_age) = self.older_than {
            if age < min_age {
                return false;
            }
        }

        true
    }

    #[inline]
    pub fn matches_path(&self, path: &Path, now: SystemTime) -> bool {
        let Ok(metadata) = std::fs::metadata(path) else {
            return false;
        };
        let Ok(modified_at) = metadata.modified() else {
            return false;
        };
        self.matches_modified(modified_at, now)
    }
}

pub fn parse_time_period(raw: &str) -> Result<Duration, String> {
    let input = raw.trim();
    if input.is_empty() {
        return Err("time period cannot be empty".to_string());
    }

    let split = input
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(input.len());
    if split == 0 {
        return Err(format!(
            "invalid time period '{raw}' (expected formats like 30m, 2h, 7d)"
        ));
    }

    let value = input[..split]
        .parse::<u64>()
        .map_err(|_| format!("invalid numeric value in time period '{raw}'"))?;
    let unit = input[split..].trim().to_ascii_lowercase();

    let unit_millis: u64 = match unit.as_str() {
        "ms" | "millisecond" | "milliseconds" => 1,
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        "w" | "week" | "weeks" => 604_800_000,
        _ => {
            return Err(format!(
                "unsupported time unit in '{raw}' (use ms, s, m, h, d, or w)"
            ));
        }
    };

    let total_millis = value
        .checked_mul(unit_millis)
        .ok_or_else(|| format!("time period is too large: '{raw}'"))?;

    Ok(Duration::from_millis(total_millis))
}
