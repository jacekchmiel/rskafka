use quote::quote;

use syn::{
    Attribute, Data,  Expr, ExprAssign, ExprLit, Lit, LitStr, Path,
};

use proc_macro2::{Span, TokenStream};
use synstructure::decl_derive;

extern crate proc_macro_error;

use proc_macro_error::proc_macro_error;
use syn::export::ToTokens;

decl_derive!([KafkaWireFormatParse, attributes(kafka_proto)] => #[proc_macro_error] wire_format_parse_derive);
decl_derive!([KafkaResponse, attributes(kafka_proto)] => #[proc_macro_error] kafka_response_derive);

decl_derive!([KafkaWireFormatWrite, attributes(kafka_proto)] => #[proc_macro_error] wire_format_write_derive);

fn wire_format_write_derive(s: synstructure::Structure) -> TokenStream {
    let serialized_size = 
    s.fold(quote!(0), |acc, bi|  quote!(#acc + #bi.serialized_size()));
    let write_into = s.each(|bi| quote!( #bi.write_into(writer)?; ));

    s.gen_impl(quote! {
        gen impl crate::wire_format::KafkaWireFormatWrite for @Self {
            fn serialized_size(&self) -> usize {
                match self { #serialized_size }
            }

            fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
                match self { #write_into }
                Ok(())
            }
        }
    })
}

fn kafka_response_derive(s: synstructure::Structure) -> TokenStream {
    let struct_ident = &s.ast().ident;

    quote! {
        impl crate::wire_format::KafkaResponse for #struct_ident {}
    }
}

// FIXME: implement with synstructure
fn wire_format_parse_derive(s: synstructure::Structure) -> TokenStream {
    let input = s.ast();
    let s = match &input.data {
        Data::Struct(s) => s,
        _ => panic!("only structures are supported"),
    };
    let struct_ident = input.ident.clone();

    let (parse, init): (Vec<TokenStream>, Vec<TokenStream>) = s
        .fields
        .iter()
        .map(|f| {
            let field_ident = f.ident.as_ref().expect("only named fields are supported");
            let wire_type = find_wire_type_attr(&f.attrs);
            // f.attrs.get(0).unwrap().
            let parse = match wire_type {
                None => quote!{
                    let (input, #field_ident) = crate::wire_format::KafkaWireFormatParse::parse_bytes(input)?;
                },

                Some(wire_type) => {
                    let err_message = LitStr::new(&format!("invalid {} value", field_ident), Span::call_site());
                    quote! {
                        let (input, #field_ident) = #wire_type::parse_bytes(input)?;  
                        use ::std::convert::TryInto;
                        let #field_ident = #field_ident.try_into().map_err(|_| crate::error::custom_error(#err_message))?;
                    }
                }   
            };

            let init = quote! { #field_ident };
            (parse, init)
        })
        .unzip();

    let out = quote! {
        impl crate::wire_format::KafkaWireFormatParse for #struct_ident {
            fn parse_bytes(input: &[u8]) -> ::nom::IResult<&[u8], Self, crate::error::ParseError> {
                #( #parse)*
                Ok((input, #struct_ident { #( #init),* }))
            }
        }
    };

    // eprintln!("TOKENS: {}", out);

    out
}

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