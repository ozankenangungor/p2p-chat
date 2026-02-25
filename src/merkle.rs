use anyhow::{anyhow, bail, Result};

pub const HASH_SIZE: usize = 32;

pub fn hash_bytes_hex(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    hex::encode(hash.as_bytes())
}

pub fn parse_hash_hex(hash: &str) -> Result<[u8; HASH_SIZE]> {
    let decoded = hex::decode(hash)?;
    if decoded.len() != HASH_SIZE {
        bail!("invalid hash length: expected {} bytes", HASH_SIZE);
    }

    let mut output = [0_u8; HASH_SIZE];
    output.copy_from_slice(&decoded);
    Ok(output)
}

pub fn compute_merkle_root_hex(chunk_hashes: &[String]) -> Result<String> {
    if chunk_hashes.is_empty() {
        return Ok(hash_bytes_hex(&[]));
    }

    let mut current: Vec<[u8; HASH_SIZE]> = chunk_hashes
        .iter()
        .map(|hash| parse_hash_hex(hash))
        .collect::<Result<Vec<_>>>()?;

    while current.len() > 1 {
        let mut next_level = Vec::with_capacity(current.len().div_ceil(2));

        let mut index = 0_usize;
        while index < current.len() {
            let left = current
                .get(index)
                .copied()
                .ok_or_else(|| anyhow!("missing left node"))?;
            let right = current.get(index + 1).copied().unwrap_or(left);

            let mut concatenated = [0_u8; HASH_SIZE * 2];
            concatenated[..HASH_SIZE].copy_from_slice(&left);
            concatenated[HASH_SIZE..].copy_from_slice(&right);
            let parent = blake3::hash(&concatenated);

            let mut parent_bytes = [0_u8; HASH_SIZE];
            parent_bytes.copy_from_slice(parent.as_bytes());
            next_level.push(parent_bytes);
            index += 2;
        }

        current = next_level;
    }

    let root = current
        .first()
        .ok_or_else(|| anyhow!("unable to compute merkle root"))?;
    Ok(hex::encode(root))
}

#[cfg(test)]
mod tests {
    use super::{compute_merkle_root_hex, hash_bytes_hex};

    #[test]
    fn merkle_root_known_input() {
        let h1 = hash_bytes_hex(b"chunk-1");
        let h2 = hash_bytes_hex(b"chunk-2");
        let h3 = hash_bytes_hex(b"chunk-3");

        let root = compute_merkle_root_hex(&vec![h1.clone(), h2.clone(), h3.clone()]);
        assert!(root.is_ok());

        if let Ok(root) = root {
            assert_eq!(
                root,
                "7a6a1a795b93bbec630ae921be2ab1be603a6a5a173c45b2291c495dcc31de81"
            );
        }
    }

    #[test]
    fn merkle_root_single_leaf_is_leaf_hash() {
        let h1 = hash_bytes_hex(b"only-leaf");
        let root = compute_merkle_root_hex(&vec![h1.clone()]);
        assert!(root.is_ok());
        if let Ok(root) = root {
            assert_eq!(root, h1);
        }
    }
}
