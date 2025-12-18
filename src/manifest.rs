use serde::{Deserialize, Serialize};

pub const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    pub file_len: u64,
    pub chunk_size: u32,
    pub chunk_hashes: Vec<String>,
    pub merkle_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filename: Option<String>,
}

impl Manifest {
    pub fn new(
        file_len: u64,
        chunk_size: u32,
        chunk_hashes: Vec<String>,
        merkle_root: String,
        original_filename: Option<String>,
    ) -> Self {
        Self {
            version: MANIFEST_VERSION,
            file_len,
            chunk_size,
            chunk_hashes,
            merkle_root,
            original_filename,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Manifest;

    #[test]
    fn manifest_roundtrip_serde() {
        let manifest = Manifest::new(
            1024,
            256,
            vec!["a".repeat(64), "b".repeat(64)],
            "c".repeat(64),
            Some("example.bin".to_string()),
        );

        let encoded = serde_json::to_vec(&manifest);
        assert!(encoded.is_ok());

        if let Ok(encoded) = encoded {
            let decoded: Result<Manifest, _> = serde_json::from_slice(&encoded);
            assert!(decoded.is_ok());
            if let Ok(decoded) = decoded {
                assert_eq!(decoded, manifest);
            }
        }
    }
}
