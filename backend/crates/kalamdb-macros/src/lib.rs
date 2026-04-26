use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    spanned::Spanned,
    Attribute, Ident, ItemStruct, LitBool, LitInt, LitStr, Result,
};

#[proc_macro_attribute]
pub fn table(attr: TokenStream, item: TokenStream) -> TokenStream {
    let table_args = parse_macro_input!(attr as TableArgs);
    let mut item_struct = parse_macro_input!(item as ItemStruct);

    let struct_ident = &item_struct.ident;

    let mut columns = Vec::new();
    let mut column_ids = std::collections::HashSet::new();
    let mut ordinals = std::collections::HashSet::new();
    for field in item_struct.fields.iter_mut() {
        let column_attr = extract_column_attr(&mut field.attrs);
        let column_attr = match column_attr {
            Some(attr) => attr,
            None => {
                return syn::Error::new_spanned(
                    &field.ident,
                    "missing #[column(...)] attribute for field",
                )
                .to_compile_error()
                .into();
            },
        };

        if let Some(err) = &column_attr.parse_error {
            return err.to_compile_error().into();
        }

        let field_ident = match &field.ident {
            Some(ident) => ident,
            None => {
                return syn::Error::new_spanned(
                    &field.ty,
                    "tuple structs are not supported by #[table]",
                )
                .to_compile_error()
                .into();
            },
        };

        if !column_ids.insert(column_attr.column_id) {
            return syn::Error::new_spanned(
                &field.ident,
                format!("duplicate column id {}", column_attr.column_id),
            )
            .to_compile_error()
            .into();
        }

        if !ordinals.insert(column_attr.ordinal_position) {
            return syn::Error::new_spanned(
                &field.ident,
                format!("duplicate column ordinal {}", column_attr.ordinal_position),
            )
            .to_compile_error()
            .into();
        }

        columns.push(ColumnSpec {
            field_name: field_ident.clone(),
            args: column_attr,
        });
    }

    columns.sort_by_key(|spec| spec.args.ordinal_position);

    let column_defs = columns.iter().map(|spec| {
        let column_id = spec.args.column_id;
        let column_name = spec.field_name.to_string();
        let ordinal_position = spec.args.ordinal_position;
        let data_type = spec.args.data_type_expr();
        let is_nullable = spec.args.is_nullable;
        let is_primary_key = spec.args.is_primary_key;
        let is_partition_key = spec.args.is_partition_key;
        let default_value = spec.args.default_expr();
        let column_comment = spec.args.comment_expr();

        quote! {
            kalamdb_commons::schemas::ColumnDefinition::new(
                #column_id,
                #column_name,
                #ordinal_position,
                #data_type,
                #is_nullable,
                #is_primary_key,
                #is_partition_key,
                #default_value,
                #column_comment,
            )
        }
    });

    let TableArgs {
        name,
        namespace,
        table_type,
        access_level,
        comment,
    } = table_args;

    if access_level.is_some() && table_type != TableKind::Shared {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[table(access_level = ...)] is only supported for shared tables",
        )
        .to_compile_error()
        .into();
    }

    let namespace_str = namespace.unwrap_or_else(|| "system".to_string());
    let namespace_expr = if namespace_str == "system" {
        quote!(kalamdb_commons::NamespaceId::system())
    } else {
        quote!(kalamdb_commons::NamespaceId::new(#namespace_str))
    };
    let table_name = name.clone();
    let namespace_for_error = namespace_str.clone();
    let table_type_expr = table_type.expr();
    let table_comment = match comment {
        Some(comment) => quote!(Some(#comment.to_string())),
        None => quote!(None),
    };
    let table_option_mutation = match access_level {
        Some(TableAccessKind::Public) => quote! {
            if let kalamdb_commons::schemas::TableOptions::Shared(options) = &mut table_def.table_options {
                options.access_level = Some(kalamdb_commons::TableAccess::Public);
            }
        },
        Some(TableAccessKind::Private) => quote! {
            if let kalamdb_commons::schemas::TableOptions::Shared(options) = &mut table_def.table_options {
                options.access_level = Some(kalamdb_commons::TableAccess::Private);
            }
        },
        Some(TableAccessKind::Restricted) => quote! {
            if let kalamdb_commons::schemas::TableOptions::Shared(options) = &mut table_def.table_options {
                options.access_level = Some(kalamdb_commons::TableAccess::Restricted);
            }
        },
        Some(TableAccessKind::Dba) => quote! {
            if let kalamdb_commons::schemas::TableOptions::Shared(options) = &mut table_def.table_options {
                options.access_level = Some(kalamdb_commons::TableAccess::Dba);
            }
        },
        None => quote! {},
    };
    let error_message =
        format!("Failed to create {}.{} table definition", namespace_for_error, table_name);

    let output = quote! {
        #item_struct

        impl #struct_ident {
            pub fn definition() -> kalamdb_commons::schemas::TableDefinition {
                let columns = vec![
                    #(#column_defs,)*
                ];

                let mut table_def = kalamdb_commons::schemas::TableDefinition::new_with_defaults(
                    #namespace_expr,
                    kalamdb_commons::TableName::new(#table_name),
                    #table_type_expr,
                    columns,
                    #table_comment,
                )
                .expect(#error_message);

                #table_option_mutation

                table_def
            }
        }
    };

    output.into()
}

struct ColumnSpec {
    field_name: Ident,
    args: ColumnArgs,
}

fn extract_column_attr(attrs: &mut Vec<Attribute>) -> Option<ColumnArgs> {
    let mut column_args = None;
    let mut new_attrs = Vec::with_capacity(attrs.len());

    for attr in attrs.drain(..) {
        if attr.path().is_ident("column") {
            match attr.parse_args::<ColumnArgs>() {
                Ok(parsed) => column_args = Some(parsed),
                Err(err) => {
                    column_args = Some(ColumnArgs::error(err));
                },
            }
        } else {
            new_attrs.push(attr);
        }
    }

    *attrs = new_attrs;
    column_args
}

#[derive(Default)]
struct TableArgs {
    name: String,
    namespace: Option<String>,
    table_type: TableKind,
    access_level: Option<TableAccessKind>,
    comment: Option<String>,
}

impl Parse for TableArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = TableArgs::default();

        while !input.is_empty() {
            let key: syn::Path = input.parse()?;
            input.parse::<syn::Token![=]>()?;

            let key_ident = key
                .get_ident()
                .ok_or_else(|| syn::Error::new_spanned(&key, "unsupported #[table] argument"))?
                .to_string();

            match key_ident.as_str() {
                "name" => {
                    let value: LitStr = input.parse()?;
                    args.name = value.value();
                },
                "namespace" => {
                    let value: LitStr = input.parse()?;
                    args.namespace = Some(value.value());
                },
                "table_type" => {
                    args.table_type = parse_table_kind_value(input)?;
                },
                "access_level" => {
                    args.access_level = Some(parse_table_access_value(input)?);
                },
                "comment" => {
                    let value: LitStr = input.parse()?;
                    args.comment = Some(value.value());
                },
                _ => {
                    return Err(syn::Error::new_spanned(&key, "unsupported #[table] argument"));
                },
            }

            if input.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            }
        }

        if args.name.is_empty() {
            return Err(syn::Error::new(input.span(), "#[table] requires name = \"...\""));
        }

        Ok(args)
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum TableKind {
    User,
    Shared,
    Stream,
    #[default]
    System,
}

impl TableKind {
    fn parse(value: &LitStr) -> Result<Self> {
        match value.value().to_lowercase().as_str() {
            "user" => Ok(Self::User),
            "shared" => Ok(Self::Shared),
            "stream" => Ok(Self::Stream),
            "system" => Ok(Self::System),
            _ => Err(syn::Error::new_spanned(
                value,
                "unsupported table_type; expected user|shared|stream|system",
            )),
        }
    }

    fn expr(&self) -> proc_macro2::TokenStream {
        match self {
            Self::User => quote!(kalamdb_commons::schemas::TableType::User),
            Self::Shared => quote!(kalamdb_commons::schemas::TableType::Shared),
            Self::Stream => quote!(kalamdb_commons::schemas::TableType::Stream),
            Self::System => quote!(kalamdb_commons::schemas::TableType::System),
        }
    }
}

#[derive(Clone, Copy)]
enum TableAccessKind {
    Public,
    Private,
    Restricted,
    Dba,
}

impl TableAccessKind {
    fn parse_str(value: &str, span: proc_macro2::Span) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "public" => Ok(Self::Public),
            "private" => Ok(Self::Private),
            "restricted" => Ok(Self::Restricted),
            "dba" => Ok(Self::Dba),
            _ => Err(syn::Error::new(
                span,
                "unsupported access_level; expected public|private|restricted|dba",
            )),
        }
    }

    fn parse(value: &LitStr) -> Result<Self> {
        Self::parse_str(&value.value(), value.span())
    }
}

fn parse_table_kind_value(input: ParseStream<'_>) -> Result<TableKind> {
    if input.peek(LitStr) {
        let value: LitStr = input.parse()?;
        return TableKind::parse(&value);
    }

    let value: syn::Path = input.parse()?;
    let ident = value
        .segments
        .last()
        .ok_or_else(|| syn::Error::new_spanned(&value, "expected table type enum variant"))?
        .ident
        .to_string();
    let lit = LitStr::new(&ident, value.span());
    TableKind::parse(&lit)
}

fn parse_table_access_value(input: ParseStream<'_>) -> Result<TableAccessKind> {
    if input.peek(LitStr) {
        let value: LitStr = input.parse()?;
        return TableAccessKind::parse(&value);
    }

    let value: syn::Path = input.parse()?;
    let ident = value
        .segments
        .last()
        .ok_or_else(|| syn::Error::new_spanned(&value, "expected access level enum variant"))?
        .ident
        .to_string();
    TableAccessKind::parse_str(&ident, value.span())
}

#[derive(Default)]
struct ColumnArgs {
    column_id: u64,
    ordinal_position: u32,
    data_type: Option<syn::Expr>,
    is_nullable: bool,
    is_primary_key: bool,
    is_partition_key: bool,
    default_value: String,
    comment: Option<String>,
    parse_error: Option<syn::Error>,
}

impl ColumnArgs {
    fn error(err: syn::Error) -> Self {
        ColumnArgs {
            parse_error: Some(err),
            ..Default::default()
        }
    }

    fn data_type_expr(&self) -> proc_macro2::TokenStream {
        if let Some(err) = &self.parse_error {
            return err.to_compile_error();
        }

        match &self.data_type {
            Some(expr) => quote!(#expr),
            None => syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[column] requires data_type = KalamDataType::...",
            )
            .to_compile_error(),
        }
    }

    fn default_expr(&self) -> proc_macro2::TokenStream {
        if let Some(err) = &self.parse_error {
            return err.to_compile_error();
        }

        match self.default_value.as_str() {
            "None" => quote!(kalamdb_commons::schemas::ColumnDefault::None),
            "Literal(false)" => quote!(kalamdb_commons::schemas::ColumnDefault::Literal(
                serde_json::Value::Bool(false)
            )),
            "Literal(true)" => quote!(kalamdb_commons::schemas::ColumnDefault::Literal(
                serde_json::Value::Bool(true)
            )),
            other if other.starts_with("Function(") && other.ends_with(')') => {
                let inner = &other["Function(".len()..other.len() - 1];
                if inner.trim().is_empty() {
                    syn::Error::new(
                        proc_macro2::Span::call_site(),
                        "Function() default requires a function name",
                    )
                    .to_compile_error()
                } else {
                    quote!(kalamdb_commons::schemas::ColumnDefault::function(#inner, vec![]))
                }
            },
            other => syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "unsupported default '{}'; expected None, Literal(true|false), or \
                     Function(NAME)",
                    other
                ),
            )
            .to_compile_error(),
        }
    }

    fn comment_expr(&self) -> proc_macro2::TokenStream {
        if let Some(err) = &self.parse_error {
            return err.to_compile_error();
        }

        match &self.comment {
            Some(comment) => quote!(Some(#comment.to_string())),
            None => quote!(None),
        }
    }
}

impl Parse for ColumnArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = ColumnArgs::default();
        let mut seen = std::collections::HashSet::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;

            let key_str = key.to_string();
            if !seen.insert(key_str.clone()) {
                return Err(syn::Error::new_spanned(key, "duplicate #[column] argument"));
            }

            if key_str == "data_type" && input.peek(syn::token::Paren) {
                let content;
                syn::parenthesized!(content in input);
                let value: syn::Expr = content.parse()?;
                args.data_type = Some(value);
            } else {
                input.parse::<syn::Token![=]>()?;

                match key_str.as_str() {
                    "id" => {
                        let value: LitInt = input.parse()?;
                        args.column_id = value.base10_parse()?;
                    },
                    "ordinal" => {
                        let value: LitInt = input.parse()?;
                        args.ordinal_position = value.base10_parse()?;
                    },
                    "data_type" => {
                        return Err(syn::Error::new_spanned(
                            key,
                            "data_type must use list syntax: data_type(KalamDataType::...)",
                        ));
                    },
                    "nullable" => {
                        let value: LitBool = input.parse()?;
                        args.is_nullable = value.value();
                    },
                    "primary_key" => {
                        let value: LitBool = input.parse()?;
                        args.is_primary_key = value.value();
                    },
                    "default" => {
                        let value: LitStr = input.parse()?;
                        args.default_value = value.value();
                    },
                    "comment" => {
                        let value: LitStr = input.parse()?;
                        args.comment = Some(value.value());
                    },
                    _ => {
                        return Err(syn::Error::new_spanned(key, "unsupported #[column] argument"));
                    },
                }
            }

            if input.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            }
        }

        if args.column_id == 0 {
            return Err(syn::Error::new(input.span(), "#[column] requires id = <u64>"));
        }

        if args.ordinal_position == 0 {
            return Err(syn::Error::new(input.span(), "#[column] requires ordinal = <u32>"));
        }

        if args.data_type.is_none() {
            return Err(syn::Error::new(
                input.span(),
                "#[column] requires data_type(KalamDataType::...)",
            ));
        }

        if args.default_value.is_empty() {
            return Err(syn::Error::new(input.span(), "#[column] requires default = \"...\""));
        }

        for required in ["nullable", "primary_key"] {
            if !seen.contains(required) {
                return Err(syn::Error::new(
                    input.span(),
                    format!("#[column] requires {} = true|false", required),
                ));
            }
        }

        Ok(args)
    }
}
