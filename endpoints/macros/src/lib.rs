//! Derive macros for `tokn-endpoint-core`.
//!
//! Currently provides [`LenientFields`], which lists a struct's
//! JSON field names (honouring `#[serde(rename = "...")]`) at compile
//! time so the lenient-deserialize helpers in `tokn-endpoint-core` know
//! which keys belong to which parameters substruct.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit, Meta};

/// Derive `tokn_endpoint_core::LenientFields` for a struct with named
/// fields. Each `pub` (or otherwise) field contributes its JSON key
/// name to the emitted `FIELDS` constant, taking
/// `#[serde(rename = "...")]` into account.
#[proc_macro_derive(LenientFields, attributes(serde))]
pub fn derive_lenient_fields(input: TokenStream) -> TokenStream {
  let input = parse_macro_input!(input as DeriveInput);
  let name = &input.ident;

  let fields = match &input.data {
    Data::Struct(s) => match &s.fields {
      Fields::Named(named) => &named.named,
      _ => {
        return syn::Error::new_spanned(name, "LenientFields can only be derived for structs with named fields")
          .to_compile_error()
          .into();
      }
    },
    _ => {
      return syn::Error::new_spanned(name, "LenientFields can only be derived for structs")
        .to_compile_error()
        .into();
    }
  };

  let mut keys: Vec<String> = Vec::new();
  for f in fields.iter() {
    let ident = match &f.ident {
      Some(i) => i,
      None => continue,
    };
    let mut key = ident.to_string();

    // Look for #[serde(rename = "...")] or #[serde(rename(deserialize = "..."))].
    for attr in &f.attrs {
      if !attr.path().is_ident("serde") {
        continue;
      }
      let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("rename") {
          // rename = "..."
          if let Ok(value) = meta.value() {
            if let Ok(Lit::Str(s)) = value.parse::<Lit>() {
              key = s.value();
            }
          } else {
            // rename(deserialize = "...", serialize = "...")
            let _ = meta.parse_nested_meta(|inner| {
              if inner.path.is_ident("deserialize") {
                if let Ok(value) = inner.value() {
                  if let Ok(Lit::Str(s)) = value.parse::<Lit>() {
                    key = s.value();
                  }
                }
              }
              Ok(())
            });
          }
        } else if meta.path.is_ident("flatten") || meta.path.is_ident("skip") {
          // We don't want flattened fields in the key list (they don't
          // correspond to a single JSON key).
          key.clear();
        }
        Ok(())
      });
    }

    if !key.is_empty() {
      keys.push(key);
    }
  }

  // Also skip if any attribute marked `#[serde(flatten)]` cleared the key.
  let lit_keys = keys.iter().map(|k| quote! { #k });

  let expanded = quote! {
    impl ::tokn_endpoint_core::LenientFields for #name {
      const FIELDS: &'static [&'static str] = &[ #( #lit_keys ),* ];
    }
  };

  expanded.into()
}

// Avoid `unused` warnings on `Meta`.
#[allow(dead_code)]
fn _unused(_: Meta) {}
