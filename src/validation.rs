use anyhow::{bail, Result};

const HASH_HEX_LENGTH: usize = 64;

pub fn validate_cid_hex(cid: &str) -> Result<()> {
    validate_hex_identifier(cid, "cid")
}

pub fn validate_chunk_hash_hex(chunk_hash: &str) -> Result<()> {
    validate_hex_identifier(chunk_hash, "chunk_hash")
}

pub fn is_valid_hex_identifier(value: &str) -> bool {
    validate_hex_identifier(value, "value").is_ok()
}

fn validate_hex_identifier(value: &str, name: &str) -> Result<()> {
    if value.len() != HASH_HEX_LENGTH {
        bail!(
            "invalid {name}: expected {HASH_HEX_LENGTH} hex chars, got {}",
            value.len()
        );
    }

    if value
        .chars()
        .any(|character| !character.is_ascii_hexdigit())
    {
        bail!("invalid {name}: value must be hexadecimal");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_chunk_hash_hex, validate_cid_hex};

    #[test]
    fn reject_invalid_hex_identifier_length() {
        let result = validate_cid_hex("abc");
        assert!(result.is_err());
    }

    #[test]
    fn reject_non_hex_identifier() {
        let result = validate_chunk_hash_hex(&"z".repeat(64));
        assert!(result.is_err());
    }
}
