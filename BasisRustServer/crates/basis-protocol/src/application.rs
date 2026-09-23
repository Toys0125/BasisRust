use crate::io::{NetReader, NetWriter};
use std::borrow::Cow;

pub const DEFAULT_COMPANY_NAME: &str = "Basis Unity";
pub const DEFAULT_PRODUCT_NAME: &str = "Basis Unity";
pub const MAX_NAME_LENGTH: usize = 64;

const TAG_RAW: u8 = 0;
const TAG_DEFAULT: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkApplication {
    pub company_name: String,
    pub product_name: String,
}

impl Default for NetworkApplication {
    fn default() -> Self {
        Self {
            company_name: DEFAULT_COMPANY_NAME.to_string(),
            product_name: DEFAULT_PRODUCT_NAME.to_string(),
        }
    }
}

impl NetworkApplication {
    pub fn matches(&self, accepted_company_name: &str, accepted_product_name: &str) -> bool {
        self.company_name == accepted_company_name && self.product_name == accepted_product_name
    }

    pub fn write(writer: &mut NetWriter, company_name: &str, product_name: &str) {
        if company_name == DEFAULT_COMPANY_NAME && product_name == DEFAULT_PRODUCT_NAME {
            writer.put_u8(TAG_DEFAULT);
            return;
        }

        writer.put_u8(TAG_RAW);
        let company_name = truncate_name(company_name);
        let product_name = truncate_name(product_name);
        writer.put_string(company_name.as_ref());
        writer.put_string(product_name.as_ref());
    }

    pub fn encode(company_name: &str, product_name: &str) -> Vec<u8> {
        let mut writer = NetWriter::with_capacity(1 + company_name.len() + product_name.len() + 4);
        Self::write(&mut writer, company_name, product_name);
        writer.into_vec()
    }

    pub fn try_read(reader: &mut NetReader<'_>) -> Option<Self> {
        match reader.get_u8().ok()? {
            TAG_DEFAULT => Some(Self::default()),
            TAG_RAW => Some(Self {
                company_name: reader.get_string().ok()?,
                product_name: reader.get_string().ok()?,
            }),
            _ => None,
        }
    }

    pub fn unsupported_reason(
        &self,
        accepted_company_name: &str,
        accepted_product_name: &str,
    ) -> String {
        format!(
            "This server only accepts company \"{}\" and product \"{}\"; your client reports company \"{}\" and product \"{}\".",
            describe(accepted_company_name),
            describe(accepted_product_name),
            describe(&self.company_name),
            describe(&self.product_name),
        )
    }
}

fn truncate_name(value: &str) -> Cow<'_, str> {
    if value.encode_utf16().count() <= MAX_NAME_LENGTH {
        return Cow::Borrowed(value);
    }

    let utf16 = value
        .encode_utf16()
        .take(MAX_NAME_LENGTH)
        .collect::<Vec<_>>();
    Cow::Owned(String::from_utf16_lossy(&utf16))
}

fn describe(value: &str) -> String {
    let clean = value
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_NAME_LENGTH)
        .collect::<String>();
    if clean.is_empty() {
        "none".to_string()
    } else {
        clean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_application_uses_compact_tag() {
        assert_eq!(
            NetworkApplication::encode(DEFAULT_COMPANY_NAME, DEFAULT_PRODUCT_NAME),
            vec![TAG_DEFAULT]
        );
    }

    #[test]
    fn raw_application_round_trips_basis_strings() {
        let encoded = NetworkApplication::encode("Example Company", "Example Product");
        assert_eq!(encoded[0], TAG_RAW);

        let mut reader = NetReader::new(&encoded);
        let decoded = NetworkApplication::try_read(&mut reader).unwrap();
        assert_eq!(decoded.company_name, "Example Company");
        assert_eq!(decoded.product_name, "Example Product");
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn raw_application_names_are_limited_to_64_utf16_code_units() {
        let company = "a".repeat(MAX_NAME_LENGTH + 5);
        let product = "😀".repeat(40);
        let encoded = NetworkApplication::encode(&company, &product);
        let mut reader = NetReader::new(&encoded);
        let decoded = NetworkApplication::try_read(&mut reader).unwrap();
        assert_eq!(decoded.company_name.len(), MAX_NAME_LENGTH);
        assert_eq!(decoded.product_name.chars().count(), 32);
    }

    #[test]
    fn unknown_application_tag_is_rejected() {
        let mut reader = NetReader::new(&[2]);
        assert!(NetworkApplication::try_read(&mut reader).is_none());
    }
}
