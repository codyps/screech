//! Faults are compiled out unless explicitly enabled. Markers make schedules deterministic.
#[cfg(feature = "fault-injection")]
pub fn point(name: &str) {
    if std::env::var("SCREECH_FAULT_POINT").as_deref() != Ok(name) {
        return;
    }
    if let Ok(after) = std::env::var("SCREECH_FAULT_AFTER") {
        static HITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let after: usize = after.parse().expect("positive fault occurrence");
        assert!(after > 0);
        if HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 != after {
            return;
        }
    }
    if let Ok(marker) = std::env::var("SCREECH_FAULT_MARKER") {
        std::fs::write(marker, name).expect("fault marker");
    }
    match std::env::var("SCREECH_FAULT_ACTION").as_deref() {
        Ok("exit") => std::process::exit(86),
        Ok("hang") => loop {
            std::thread::park();
        },
        _ => (),
    }
}
#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn point(_: &str) {}

#[cfg(feature = "fault-injection")]
pub fn operation(stage: &str, kind: &str) {
    point(&format!("{stage}_{kind}_commit"));
}
#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn operation(_: &str, _: &str) {}

/// Describe only the test batch whose commit boundary is about to be interrupted.
/// No context files or environment access exist in a production build.
#[cfg(feature = "fault-injection")]
pub fn batch_point(stage: &str, event: &crate::durable::Event) {
    use crate::durable::Event;
    let kind = match event {
        Event::AckBatch { .. } => "ack",
        Event::DeliverBatch { .. } => "delivery",
        _ => return,
    };
    let name = format!("{stage}_{kind}_batch_commit");
    if std::env::var("SCREECH_FAULT_POINT").as_deref() != Ok(&name) {
        return;
    }
    if let Ok(marker) = std::env::var("SCREECH_FAULT_MARKER") {
        let sequences: Vec<_> = match event {
            Event::DeliverBatch { deliveries, .. } => {
                deliveries.iter().map(|d| d.sequence).collect()
            }
            Event::AckBatch { events } => events
                .iter()
                .map(|event| {
                    let Event::Ack { sequence, .. } = event else {
                        unreachable!()
                    };
                    *sequence
                })
                .collect(),
            _ => unreachable!(),
        };
        std::fs::write(
            format!("{marker}.context"),
            serde_json::to_vec(&sequences).unwrap(),
        )
        .expect("batch context");
    }
    point(&name);
}
#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn batch_point(_: &str, _: &crate::durable::Event) {}

#[cfg(feature = "fault-injection")]
pub fn group_gate() {
    let Ok(path) = std::env::var("SCREECH_GROUP_GATE") else {
        return;
    };
    if std::path::Path::new(&format!("{path}.release")).exists() {
        return;
    }
    std::fs::write(format!("{path}.ready"), b"ready").expect("group gate marker");
    while !std::path::Path::new(&format!("{path}.release")).exists() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn group_gate() {}

#[cfg(feature = "fault-injection")]
pub fn group_ingress() {
    use std::io::Write;
    if let Ok(path) = std::env::var("SCREECH_GROUP_GATE") {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{path}.ingress"))
            .expect("group ingress");
        file.write_all(b".").expect("group ingress marker");
    }
}
#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn group_ingress() {}

#[cfg(feature = "fault-injection")]
pub fn group_context(
    base: u64,
    records: &[crate::storage::Append],
    events: &[crate::durable::Event],
) {
    if std::env::var("SCREECH_FAULT_POINT").is_err() {
        return;
    }
    if let Ok(marker) = std::env::var("SCREECH_FAULT_MARKER") {
        if std::env::var("SCREECH_FAULT_POINT").as_deref() == Ok("capture_group")
            && std::path::Path::new(&format!("{marker}.group")).exists()
        {
            return;
        }
        let rows: Vec<_> = records.iter().zip(events).map(|(r, event)| {
            let payload = match event {
                crate::durable::Event::Publish { message, .. } => &message.payload,
                _ => &r.payload,
            };
            serde_json::json!({
                "event":serde_json::from_slice::<serde_json::Value>(&r.headers).unwrap(), "payload":payload.as_ref()
            })
        }).collect();
        std::fs::write(
            format!("{marker}.group"),
            serde_json::to_vec(&serde_json::json!({"base":base,"records":rows})).unwrap(),
        )
        .expect("group context");
    }
}

/// Pause after the initial projection, before admitting arrivals during assembly.
#[cfg(feature = "fault-injection")]
pub fn group_extend_gate() {
    let Ok(path) = std::env::var("SCREECH_GROUP_EXTEND_GATE") else {
        return;
    };
    if std::path::Path::new(&format!("{path}.release")).exists() {
        return;
    }
    std::fs::write(format!("{path}.ready"), b"ready").expect("group extension marker");
    while !std::path::Path::new(&format!("{path}.release")).exists() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn group_extend_gate() {}
