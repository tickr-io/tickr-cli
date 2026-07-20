//! Identity code: a short, human-legible handle for a graph structure (a
//! `Node` or an `Edge`), projected from its UUID.
//!
//! The code is the first four Crockford base32 symbols of the structure's
//! UUID — the most-significant 20 bits, five bits per symbol. It is a pure
//! projection: no stored field, no minting, no collision handling. It is
//! derived on read wherever the live graph is exposed (the HTTP instance view,
//! the ctx graph), with the full UUID travelling alongside as the unambiguous
//! fallback.
//!
//! It lives here, in the published contract crate, because both planes must
//! compute the same codes independently: the server derives one inside the
//! Instance snapshot and the API component re-derives one for replay
//! `resume_from` refs. A single implementation guarantees they never diverge.
//!
//! Two consequences fall straight out of the projection being a deterministic
//! function of the UUID, with no upstream work needed:
//! - a base-graph structure keeps one code across every instance of a
//!   definition version, because it keeps one UUID; and
//! - a patch-added structure gets a fresh code the moment it lands, because it
//!   carries a fresh UUID.
//!
//! The Crockford base32 alphabet omits the confusable letters `I`, `L`, `O`,
//! and `U`, which is what makes a four-symbol code safe to read aloud or copy
//! by hand — the whole point of surfacing a handle shorter than the 36-char
//! UUID.

use uuid::Uuid;

/// Crockford base32 encode alphabet: `0-9` then `A-Z` with `I`, `L`, `O`, `U`
/// removed. Index a symbol by its 5-bit value.
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// The number of Crockford symbols in an identity code.
const CODE_LEN: u32 = 4;

/// Project a UUID to its four-character identity code.
///
/// Deterministic: the same UUID always yields the same code, so a structure's
/// code is stable for exactly as long as its UUID is.
pub fn identity_code(uuid: &Uuid) -> String {
    let bits = uuid.as_u128();
    let mut code = String::with_capacity(CODE_LEN as usize);
    for i in 0..CODE_LEN {
        // Walk the most-significant 20 bits five at a time, high symbol first.
        let shift = 128 - 5 * (i + 1);
        let index = ((bits >> shift) & 0x1f) as usize;
        code.push(CROCKFORD[index] as char);
    }
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_is_four_crockford_symbols() {
        let code = identity_code(&Uuid::new_v4());
        assert_eq!(code.len(), 4, "one symbol per 5 bits over the top 20 bits");
        assert!(
            code.bytes().all(|b| CROCKFORD.contains(&b)),
            "every symbol comes from the Crockford alphabet: {code}"
        );
    }

    #[test]
    fn projection_is_deterministic() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            identity_code(&uuid),
            identity_code(&uuid),
            "the same UUID must always project to the same code"
        );
    }

    #[test]
    fn code_reads_the_most_significant_20_bits() {
        // Top 20 bits: 00000 00001 00010 00011 -> indices 0,1,2,3 -> "0123".
        // That is leading hex 0x00443; the remaining bits are noise the
        // projection must ignore, so fill them with ones.
        let uuid = Uuid::from_u128(0x0044_3fff_ffff_ffff_ffff_ffff_ffff_ffffu128);
        assert_eq!(identity_code(&uuid), "0123");
    }

    #[test]
    fn top_of_the_alphabet_maps_to_z() {
        // Top 20 bits all ones -> index 31 four times -> "ZZZZ".
        let uuid = Uuid::from_u128(0xFFFF_F000_0000_0000_0000_0000_0000_0000u128);
        assert_eq!(identity_code(&uuid), "ZZZZ");
    }

    #[test]
    fn distinct_leading_bits_yield_distinct_codes() {
        let a = Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000u128);
        let b = Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000u128);
        assert_ne!(identity_code(&a), identity_code(&b));
    }

    #[test]
    fn confusable_letters_never_appear() {
        // The alphabet excludes I, L, O, U by construction; assert the source
        // of truth so a mis-typed alphabet is caught here rather than in a code.
        for bad in [b'I', b'L', b'O', b'U'] {
            assert!(
                !CROCKFORD.contains(&bad),
                "Crockford alphabet must omit the confusable letter {}",
                bad as char
            );
        }
    }
}
