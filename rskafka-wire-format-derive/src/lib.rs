use quote::quote;

use syn::{Attribute, Data, Expr, ExprAssign, ExprLit, Lit, LitStr, Path};

use proc_macro2::{Span, TokenStream};
use synstructure::{decl_derive, VariantInfo};

extern crate proc_macro_error;

use proc_macro_error::proc_macro_error;
use syn::export::ToTokens;

fn wire_format_write_derive(s: synstructure::Structure) -> TokenStream {
    let wire_size = s.fold(quote!(0), |acc, bi| quote!(#acc + #bi.wire_size()));
    let write_into = s.each(|bi| quote!( #bi.write_into(writer)?; ));

    s.gen_impl(quote! {
        gen impl rskafka_wire_format::prelude::WireFormatWrite for @Self {
            fn wire_size(&self) -> usize {
                match self { #wire_size }
            }

            fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
                match self { #write_into }
                Ok(())
            }
        }
    })
}
decl_derive!([WireFormatWrite, attributes(kafka_proto)] => #[proc_macro_error] wire_format_write_derive);

fn wire_format_parse_derive(s: synstructure::Structure) -> TokenStream {
    match &s.ast().data {
        Data::Struct(_) => (),
        _ => panic!("only structures are supported"),
    };
    let variant: &VariantInfo = &s.variants()[0];

    let parse = variant.bindings().iter().map(|bi| {
        let field = bi.ast();
        let name = field.ident.as_ref().unwrap();
        let custom_wire_type = find_wire_type_attr(&field.attrs);
        let err_context = LitStr::new(&format!("{}", name), Span::call_site());
        match custom_wire_type {
            None => quote!(let (input, #name) = ::rskafka_wire_format::WireFormatParse::parse(input).map_err(|e| e.map(|e| e.context(#err_context.into())))?;),
            Some(wire_type) => {
                let err_message = LitStr::new(&format!("invalid {} value", name), Span::call_site());
                quote! {
                    let (input, #name) = #wire_type::parse(input).map_err(|e| e.map(|e| e.context(#err_context.into())))?;
                    let #name = #name.try_into().map_err(|_| ::rskafka_wire_format::error::custom_error(#err_message))?;
                }
            },
        }
    });

    let construct = variant.construct(|f, _| {
        let name = f.ident.as_ref().unwrap();
        quote!( #name )
    });

    s.gen_impl(quote! {
        extern crate std;
        use std::convert::TryInto as TryInto__RskafkaProtoDerive;

        gen impl rskafka_wire_format::prelude::WireFormatParse for @Self {
            fn parse(input: &[u8]) -> ::rskafka_wire_format::IResult<&[u8], Self, ::rskafka_wire_format::error::ParseError> {
                #(#parse)*
                Ok((input, #construct))
            }
        }
    })
}
decl_derive!([WireFormatParse, attributes(kafka_proto)] => #[proc_macro_error] wire_format_parse_derive);

fn find_wire_type_attr(attributes: &[Attribute]) -> Option<Path> {
    let arg: ExprAssign = attributes
        .into_iter()
        .find(|attr| attr.path.to_token_stream().to_string() == "kafka_proto")
        .map(|attr| attr.parse_args().expect("failed to parse attribute args"))?;

    if get_attr_left(&arg) != "wire_type" {
        panic!("expected attr in the form of `wire_type = ...`");
    }

    Some(get_attr_right(&arg))
}

fn get_attr_left(expr: &ExprAssign) -> String {
    match expr.left.as_ref() {
        Expr::Path(p) => p.path.get_ident().expect("invalid attr left").to_string(),
        _ => panic!("invalid attr left"),
    }
}

fn get_attr_right(expr: &ExprAssign) -> Path {
    let literal = extract_str_literal(expr.right.as_ref());
    literal.parse().unwrap_or_else(|e| panic!(e))
}

fn extract_str_literal(expr: &Expr) -> &LitStr {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(str_lit),
            ..
        }) => &str_lit,
        _ => panic!("expected string literal"),
    }
}
