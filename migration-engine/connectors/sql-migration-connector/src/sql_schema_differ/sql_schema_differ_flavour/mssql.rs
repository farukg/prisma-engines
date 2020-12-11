use super::SqlSchemaDifferFlavour;
use crate::sql_schema_differ::column::ColumnDiffer;
use crate::sql_schema_differ::column::ColumnTypeChange;
use crate::{flavour::MssqlFlavour, sql_schema_differ::SqlSchemaDiffer};
use native_types::{MsSqlType, MsSqlTypeParameter};
use sql_schema_describer::walkers::IndexWalker;
use sql_schema_describer::ColumnTypeFamily;
use std::collections::HashSet;

impl SqlSchemaDifferFlavour for MssqlFlavour {
    fn should_skip_index_for_new_table(&self, index: &IndexWalker<'_>) -> bool {
        index.index_type().is_unique()
    }

    fn should_recreate_the_primary_key_on_column_recreate(&self) -> bool {
        true
    }

    fn tables_to_redefine(&self, differ: &SqlSchemaDiffer<'_>) -> HashSet<String> {
        let autoincrement_changed = differ
            .table_pairs()
            .filter(|differ| differ.column_pairs().any(|c| c.autoincrement_changed()))
            .map(|table| table.next().name().to_owned());

        let all_columns_of_the_table_gets_dropped = differ
            .table_pairs()
            .filter(|tables| {
                tables.column_pairs().all(|columns| {
                    let type_changed = columns.previous.column_type_family() != columns.next.column_type_family();
                    let not_castable = matches!(type_change_riskyness(&columns), ColumnTypeChange::NotCastable);

                    type_changed && not_castable
                })
            })
            .map(|tables| tables.previous().name().to_string());

        autoincrement_changed
            .chain(all_columns_of_the_table_gets_dropped)
            .collect()
    }

    fn column_type_change(&self, differ: &ColumnDiffer<'_>) -> Option<ColumnTypeChange> {
        if differ.previous.column_type_family() == differ.next.column_type_family() {
            None
        } else {
            Some(type_change_riskyness(differ))
        }
    }
}

fn type_change_riskyness(differ: &ColumnDiffer<'_>) -> ColumnTypeChange {
    match (differ.previous.column_type_family(), differ.next.column_type_family()) {
        (_, ColumnTypeFamily::String) => ColumnTypeChange::SafeCast,
        (ColumnTypeFamily::String, ColumnTypeFamily::Int)
        | (ColumnTypeFamily::DateTime, ColumnTypeFamily::Float)
        | (ColumnTypeFamily::String, ColumnTypeFamily::Float) => ColumnTypeChange::NotCastable,
        (_, _) => ColumnTypeChange::RiskyCast,
    }
}

fn native_type_change_riskyness(differ: &ColumnDiffer<'_>) -> ColumnTypeChange {
    use ColumnTypeChange::*;
    use MsSqlTypeParameter::*;

    let (previous_type, next_type): (Option<MsSqlType>, Option<MsSqlType>) =
        (differ.previous.column_native_type(), differ.next.column_native_type());

    match (previous_type, next_type) {
        (None, _) | (_, None) => type_change_riskyness(differ),

        (Some(MsSqlType::Bit), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => SafeCast,
            MsSqlType::SmallInt => SafeCast,
            MsSqlType::Int => SafeCast,
            MsSqlType::BigInt => SafeCast,
            MsSqlType::Decimal(_) => SafeCast,
            MsSqlType::Numeric(_) => SafeCast,
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => SafeCast,
            MsSqlType::Float(_) => SafeCast,
            MsSqlType::Real => SafeCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::SmallDateTime => SafeCast,
            MsSqlType::Binary(_) => SafeCast,
            MsSqlType::VarBinary(_) => SafeCast,
            MsSqlType::Bit => SafeCast,
            MsSqlType::Char(_) => SafeCast,
            MsSqlType::NChar(_) => SafeCast,
            MsSqlType::VarChar(_) => SafeCast,
            MsSqlType::NVarChar(_) => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::TinyInt), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => SafeCast,
            MsSqlType::SmallInt => SafeCast,
            MsSqlType::Int => SafeCast,
            MsSqlType::BigInt => SafeCast,
            MsSqlType::Decimal(params) => match params {
                // TinyInt can be at most three digits, so this might fail.
                Some((p, s)) if p - s < 3 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Numeric(params) => match params {
                // TinyInt can be at most three digits, so this might fail.
                Some((p, s)) if p - s < 3 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => SafeCast,
            MsSqlType::Float(_) => SafeCast,
            MsSqlType::Real => SafeCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::SmallDateTime => SafeCast,
            MsSqlType::Binary(_) => SafeCast,
            MsSqlType::VarBinary(_) => SafeCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // TinyInt can be at most three digits, so this might fail.
                Some(len) if len < 3 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // TinyInt can be at most three digits, so this might fail.
                Some(Number(len)) if len < 3 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::SmallInt), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => SafeCast,
            MsSqlType::Int => SafeCast,
            MsSqlType::BigInt => SafeCast,
            MsSqlType::Decimal(params) => match params {
                // SmallInt can be at most five digits, so this might fail.
                Some((p, s)) if p - s < 5 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Numeric(params) => match params {
                // SmallInt can be at most five digits, so this might fail.
                Some((p, s)) if p - s < 5 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => SafeCast,
            MsSqlType::Float(_) => SafeCast,
            MsSqlType::Real => SafeCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::SmallDateTime => SafeCast,
            MsSqlType::Binary(param) => match param {
                // SmallInt is two bytes, so this might fail.
                Some(n) if n < 2 => RiskyCast,
                None => RiskyCast, // n == 1 by default
                _ => SafeCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // SmallInt is two bytes, so this might fail.
                Some(Number(n)) if n < 2 => RiskyCast,
                None => RiskyCast, // n == 1 by default
                _ => SafeCast,
            },
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have five digits and an optional sign.
                Some(len) if len < 6 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have five digits and an optional sign.
                Some(Number(len)) if len < 6 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::Int), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => SafeCast,
            MsSqlType::BigInt => SafeCast,
            MsSqlType::Decimal(params) => match params {
                // Int can be at most ten digits, so this might fail.
                Some((p, s)) if p - s < 10 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Numeric(params) => match params {
                // Int can be at most ten digits, so this might fail.
                Some((p, s)) if p - s < 10 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => SafeCast,
            MsSqlType::Float(_) => SafeCast,
            MsSqlType::Real => SafeCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::SmallDateTime => SafeCast,
            MsSqlType::Binary(param) => match param {
                // Int is four bytes.
                Some(n) if n < 4 => RiskyCast,
                None => RiskyCast, // n == 1 by default
                _ => SafeCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // Int is four bytes.
                Some(Number(n)) if n < 4 => RiskyCast,
                None => RiskyCast, // n == 1 by default
                _ => SafeCast,
            },
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // Int can be at most eleven characters, so this might fail.
                Some(len) if len < 11 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // Int can be at most eleven characters, so this might fail.
                Some(Number(len)) if len < 11 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::BigInt), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => SafeCast,
            MsSqlType::Decimal(params) => match params {
                // BigInt can have at most 19 digits.
                Some((p, s)) if p - s < 19 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Numeric(params) => match params {
                // BigInt can have at most 19 digits.
                Some((p, s)) if p - s < 19 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => SafeCast,
            MsSqlType::Float(_) => SafeCast,
            MsSqlType::Real => SafeCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::SmallDateTime => SafeCast,
            MsSqlType::Binary(param) => match param {
                // BigInt is eight bytes.
                Some(n) if n < 8 => RiskyCast,
                None => RiskyCast, // n == 1 by default
                _ => SafeCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // BigInt is eight bytes.
                Some(Number(n)) if n < 8 => RiskyCast,
                None => RiskyCast, // n == 1 by default
                _ => SafeCast,
            },
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // BigInt can have at most 20 characters.
                Some(len) if len < 20 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // BigInt can have at most 20 characters.
                Some(Number(len)) if len < 20 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::Decimal(params)), Some(new_type)) | (Some(MsSqlType::Numeric(params)), Some(new_type)) => {
            match new_type {
                MsSqlType::TinyInt => RiskyCast,
                MsSqlType::SmallInt => RiskyCast,
                MsSqlType::Int => RiskyCast,
                MsSqlType::BigInt => RiskyCast,
                MsSqlType::Numeric(_) => SafeCast,
                MsSqlType::Money => RiskyCast,
                MsSqlType::SmallMoney => RiskyCast,
                MsSqlType::Bit => RiskyCast,
                MsSqlType::Float(_) => RiskyCast,
                MsSqlType::Real => RiskyCast,
                MsSqlType::Date => NotCastable,
                MsSqlType::Time => NotCastable,
                MsSqlType::DateTime => RiskyCast,
                MsSqlType::SmallDateTime => RiskyCast,
                MsSqlType::Binary(_) => RiskyCast,
                MsSqlType::VarBinary(_) => RiskyCast,
                MsSqlType::Decimal(_) => SafeCast,
                MsSqlType::Char(length) | MsSqlType::NChar(length) => match (length, params) {
                    // We must fit p characters to our string, otherwise might
                    // truncate.
                    (Some(len), Some((p, 0))) if p < len => RiskyCast,
                    // We must fit p character and a comma to our string,
                    // otherwise might truncate.
                    (Some(len), Some((p, _))) if p + 1 < len => RiskyCast,
                    // Defaults to `number(18, 0)`, so we must fit 18 characters
                    // without truncating.
                    (Some(len), None) if len < 18 => RiskyCast,
                    // Defaults to one character, so we can fit one digit
                    // without truncating.
                    (None, Some((p, 0))) if p > 1 => RiskyCast,
                    (None, Some(_)) => RiskyCast,
                    (None, None) => RiskyCast,
                    _ => SafeCast,
                },
                MsSqlType::VarChar(length) | MsSqlType::NVarChar(length) => match (length, params) {
                    // We must fit p characters to our string, otherwise might
                    // truncate.
                    (Some(Number(len)), Some((p, 0))) if p < len.into() => RiskyCast,
                    // We must fit p character and a comma to our string,
                    // otherwise might truncate.
                    (Some(Number(len)), Some((p, _))) if p + 1 < len.into() => RiskyCast,
                    // Defaults to `number(18, 0)`, so we must fit 18 characters
                    // without truncating.
                    (Some(Number(len)), None) if len < 18 => RiskyCast,
                    // Defaults to one character, so we can fit one digit
                    // without truncating.
                    (None, Some((p, 0))) if p > 1 => RiskyCast,
                    (None, Some(_)) => RiskyCast,
                    (None, None) => RiskyCast,
                    _ => SafeCast,
                },
                _ => NotCastable,
            }
        }

        (Some(MsSqlType::Money), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(params) | MsSqlType::Numeric(params) => match params {
                // We can have 19 digits and four decimals.
                Some((p, s)) if p < 19 || s < 4 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(_) => RiskyCast,
            MsSqlType::Real => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 19 digits, comma and sign
                Some(len) if len < 21 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 19 digits, comma and sign
                Some(Number(len)) if len < 21 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::UniqueIdentifier => NotCastable,
            MsSqlType::Binary(param) => match param {
                // Eight bytes.
                Some(len) if len < 8 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // Eight bytes.
                Some(Number(len)) if len < 8 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::SmallMoney), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(params) | MsSqlType::Numeric(params) => match params {
                // Ten digits, four decimals
                Some((p, s)) if p < 10 || s < 4 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => SafeCast,
            MsSqlType::SmallMoney => SafeCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(_) => RiskyCast,
            MsSqlType::Real => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // Ten digits, comma and a sign.
                Some(len) if len < 12 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // Ten digits, comma and a sign.
                Some(Number(len)) if len < 12 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::UniqueIdentifier => NotCastable,
            MsSqlType::Binary(param) => match param {
                // Four bytes.
                Some(len) if len < 4 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // Four bytes.
                Some(Number(len)) if len < 4 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::Float(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(_) => RiskyCast,
            MsSqlType::Numeric(_) => RiskyCast,
            MsSqlType::Money => RiskyCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(new_param) => match (old_param, new_param) {
                // If length is 24 or lower, we have a four byte float.
                (Some(left_len), Some(right_len)) if left_len <= 24 && right_len <= 24 => SafeCast,
                (Some(left_len), Some(right_len)) if left_len > 24 && right_len > 24 => SafeCast,
                // If length is not set, it's an eight byte float (double).
                (None, Some(right_len)) if right_len > 24 => SafeCast,
                (Some(left_len), None) if left_len > 24 => SafeCast,
                (None, None) => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Real => match old_param {
                // Real is always a four byte float.
                Some(len) if len <= 24 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::Char(new_param) | MsSqlType::NChar(new_param) => match (old_param, new_param) {
                // If float, we can have 47 characters including the sign and comma.
                (Some(f_len), Some(char_len)) if f_len <= 24 && char_len >= 47 => SafeCast,
                // If double, we can have 317 characters including the sign and comma.
                (Some(f_len), Some(char_len)) if f_len > 24 && char_len >= 317 => SafeCast,
                // If length not set, it's a double.
                (None, Some(char_len)) if char_len >= 317 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(new_param) | MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                // If float, we can have 47 characters including the sign and comma.
                (Some(f_len), Some(Number(char_len))) if f_len <= 24 && char_len >= 47 => SafeCast,
                // If double, we can have 317 characters including the sign and comma.
                (Some(f_len), Some(Number(char_len))) if f_len > 24 && char_len >= 317 => SafeCast,
                // If length not set, it's a double.
                (None, Some(Number(char_len))) if char_len >= 317 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Binary(new_param) => match (old_param, new_param) {
                // Float is four bytes.
                (Some(f_len), Some(bin_len)) if f_len <= 24 && bin_len >= 4 => SafeCast,
                // Double is eight bytes.
                (Some(f_len), Some(bin_len)) if f_len > 24 && bin_len >= 8 => SafeCast,
                // By default, we have a double.
                (None, Some(bin_len)) if bin_len >= 8 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarBinary(new_param) => match (old_param, new_param) {
                // Float is four bytes.
                (Some(f_len), Some(Number(bin_len))) if f_len <= 24 && bin_len >= 4 => SafeCast,
                // Double is eight bytes.
                (Some(f_len), Some(Number(bin_len))) if f_len > 24 && bin_len >= 8 => SafeCast,
                // By default, we have a double.
                (None, Some(Number(bin_len))) if bin_len >= 8 => SafeCast,
                _ => RiskyCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::Real), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(_) => RiskyCast,
            MsSqlType::Numeric(_) => RiskyCast,
            MsSqlType::Money => RiskyCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(param) => match param {
                // Real is the same as float(24) or lower.
                Some(len) if len <= 24 => SafeCast,
                _ => RiskyCast,
            },

            MsSqlType::Real => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We have 47 characters maximum.
                Some(len) if len >= 47 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We have 47 characters maximum.
                Some(Number(len)) if len >= 47 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Binary(param) => match param {
                // Real is four bytes.
                Some(len) if len >= 4 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // Real is four bytes.
                Some(Number(len)) if len >= 4 => SafeCast,
                _ => RiskyCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::Date), Some(new_type)) => match new_type {
            MsSqlType::Date => SafeCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::DateTime2 => SafeCast,
            MsSqlType::DateTimeOffset => SafeCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 10 characters.
                Some(len) if len >= 10 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 10 characters.
                Some(Number(len)) if len >= 10 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::SmallDateTime => RiskyCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::Time), Some(new_type)) => match new_type {
            MsSqlType::Time => SafeCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => SafeCast,
            MsSqlType::DateTimeOffset => SafeCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 8 characters.
                Some(len) if len >= 8 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 8 characters.
                Some(Number(len)) if len >= 8 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::SmallDateTime => RiskyCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::DateTime), Some(new_type)) => match new_type {
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::DateTime2 => SafeCast,
            MsSqlType::DateTimeOffset => SafeCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 23 characters.
                Some(len) if len >= 23 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 23 characters.
                Some(Number(len)) if len >= 23 => SafeCast,
                _ => RiskyCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::DateTime2), Some(new_type)) => match new_type {
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => SafeCast,
            MsSqlType::DateTimeOffset => SafeCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 27 characters.
                Some(len) if len >= 27 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 27 characters.
                Some(Number(len)) if len >= 27 => SafeCast,
                _ => RiskyCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::DateTimeOffset), Some(new_type)) => match new_type {
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => RiskyCast,
            MsSqlType::DateTimeOffset => SafeCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 33 characters.
                Some(len) if len >= 33 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 33 characters.
                Some(Number(len)) if len >= 33 => SafeCast,
                _ => RiskyCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::SmallDateTime), Some(new_type)) => match new_type {
            MsSqlType::Date => SafeCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => SafeCast,
            MsSqlType::DateTime2 => SafeCast,
            MsSqlType::DateTimeOffset => SafeCast,
            MsSqlType::SmallDateTime => SafeCast,
            MsSqlType::Char(param) | MsSqlType::NChar(param) => match param {
                // We can have 19 characters.
                Some(len) if len >= 19 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::VarChar(param) | MsSqlType::NVarChar(param) => match param {
                // We can have 19 characters.
                Some(Number(len)) if len >= 19 => SafeCast,
                _ => RiskyCast,
            },
            _ => NotCastable,
        },

        (Some(MsSqlType::Char(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(_) => RiskyCast,
            MsSqlType::Numeric(_) => RiskyCast,
            MsSqlType::Money => RiskyCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(_) => RiskyCast,
            MsSqlType::Real => RiskyCast,
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => RiskyCast,
            MsSqlType::DateTimeOffset => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(new_param) | MsSqlType::NChar(new_param) => match (old_param, new_param) {
                (Some(old_len), Some(new_len)) if old_len > new_len => RiskyCast,
                // Default length is 1.
                (Some(old_len), None) if old_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(new_param) | MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                (Some(old_len), Some(Number(new_len))) if u32::from(old_len) > new_len => RiskyCast,
                // Default length is 1.
                (Some(old_len), None) if old_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Text => SafeCast,
            MsSqlType::NText => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::NChar(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(_) => RiskyCast,
            MsSqlType::Numeric(_) => RiskyCast,
            MsSqlType::Money => RiskyCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(_) => RiskyCast,
            MsSqlType::Real => RiskyCast,
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => RiskyCast,
            MsSqlType::DateTimeOffset => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::NChar(new_param) => match (old_param, new_param) {
                (Some(old_len), Some(new_len)) if old_len > new_len => RiskyCast,
                // Default length is 1.
                (Some(old_len), None) if old_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Char(_) => RiskyCast,
            MsSqlType::VarChar(_) => RiskyCast,
            MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                (Some(old_len), Some(Number(new_len))) if u32::from(old_len) > new_len => RiskyCast,
                // Default length is 1.
                (Some(old_len), None) if old_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Text => RiskyCast,
            MsSqlType::NText => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::VarChar(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(_) => RiskyCast,
            MsSqlType::Numeric(_) => RiskyCast,
            MsSqlType::Money => RiskyCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(_) => RiskyCast,
            MsSqlType::Real => RiskyCast,
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => RiskyCast,
            MsSqlType::DateTimeOffset => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::NChar(new_param) | MsSqlType::Char(new_param) => match (old_param, new_param) {
                (Some(Number(old_len)), Some(new_len)) if old_len > new_len.into() => RiskyCast,
                // Default length is 1.
                (Some(Number(old_len)), None) if old_len > 1 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(new_param) | MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                (Some(Number(old_len)), Some(Number(new_len))) if old_len > new_len => RiskyCast,
                // Default length is 1.
                (Some(Number(old_len)), None) if old_len > 1 => RiskyCast,
                (Some(Max), Some(Number(_))) => RiskyCast,
                (Some(Max), None) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Text => SafeCast,
            MsSqlType::NText => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::NVarChar(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => RiskyCast,
            MsSqlType::SmallInt => RiskyCast,
            MsSqlType::Int => RiskyCast,
            MsSqlType::BigInt => RiskyCast,
            MsSqlType::Decimal(_) => RiskyCast,
            MsSqlType::Numeric(_) => RiskyCast,
            MsSqlType::Money => RiskyCast,
            MsSqlType::SmallMoney => RiskyCast,
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(_) => RiskyCast,
            MsSqlType::Real => RiskyCast,
            MsSqlType::Date => RiskyCast,
            MsSqlType::Time => RiskyCast,
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::DateTime2 => RiskyCast,
            MsSqlType::DateTimeOffset => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(_) => RiskyCast,
            MsSqlType::VarChar(_) => RiskyCast,
            MsSqlType::NChar(new_param) => match (old_param, new_param) {
                (Some(Number(old_len)), Some(new_len)) if old_len > new_len.into() => RiskyCast,
                // Default length is 1.
                (Some(Number(old_len)), None) if old_len > 1 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                (Some(Number(old_len)), Some(Number(new_len))) if old_len > new_len => RiskyCast,
                // Default length is 1.
                (Some(Number(old_len)), None) if new_len > 1 => RiskyCast,
                (Some(Max), Some(Number(_))) => RiskyCast,
                (Some(Max), None) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Text => RiskyCast,
            MsSqlType::NText => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::Text), Some(new_type)) => match new_type {
            MsSqlType::Char(_) => RiskyCast,
            MsSqlType::NChar(_) => RiskyCast,
            MsSqlType::VarChar(param) => match param {
                Some(Max) => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Text => SafeCast,
            // NVarChar uses double the space, meaning we have half the amount
            // of characters available. This transformation might fail.
            MsSqlType::NVarChar(_) => RiskyCast,
            // NText uses double the space, meaning we have half the amount
            // of characters available. This transformation might fail.
            MsSqlType::NText => RiskyCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::NText), Some(new_type)) => match new_type {
            MsSqlType::Char(_) => RiskyCast,
            MsSqlType::NChar(_) => RiskyCast,
            MsSqlType::VarChar(_) => RiskyCast,
            MsSqlType::Text => SafeCast,
            MsSqlType::NVarChar(param) => match param {
                Some(Max) => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::NText => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::Binary(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => match old_param {
                // One byte for tinyint.
                Some(len) if len <= 1 => SafeCast,
                None => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::SmallInt => match old_param {
                // Two bytes for smallint.
                Some(len) if len <= 2 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Int => match old_param {
                // Four bytes for int.
                Some(len) if len <= 4 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::BigInt => match old_param {
                // Eight bytes for bigint.
                Some(len) if len <= 8 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Decimal(new_params) | MsSqlType::Numeric(new_params) => match (old_param, new_params) {
                // If precision is 9 or less, we can fit at most four bytes.
                (Some(len), Some((p, _))) if p <= 9 && len > 4 => RiskyCast,
                // If precision is 19 or less, we can fit at most eight bytes.
                (Some(len), Some((p, _))) if p <= 19 && len > 8 => RiskyCast,
                // If precision is 28 or less, we can fit at most twelve bytes.
                (Some(len), Some((p, _))) if p <= 28 && len > 12 => RiskyCast,
                // If precision is 38 or less, we can fit at most sixteen bytes.
                (Some(len), Some((p, _))) if p <= 38 && len > 16 => RiskyCast,
                // Default precision is 18, so we can have at most eight bytes.
                (Some(len), None) if len > 8 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => match old_param {
                // We can fit at most eight bytes here.
                Some(len) if len > 8 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::SmallMoney => match old_param {
                // We can fit at most four bytes here.
                Some(len) if len > 4 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(new_param) => match (old_param, new_param) {
                // Float is four bytes.
                (Some(binary_len), Some(float_len)) if float_len <= 24 && binary_len > 4 => RiskyCast,
                // Double is eight bytes.
                (Some(binary_len), Some(float_len)) if float_len <= 53 && binary_len > 8 => RiskyCast,
                // By default we have a double.
                (Some(binary_len), None) if binary_len > 8 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Real => match old_param {
                // Float is four bytes.
                Some(binary_len) if binary_len > 4 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(new_param) => match (old_param, new_param) {
                (Some(binary_len), Some(char_len)) if binary_len > char_len => RiskyCast,
                // Default Char length is one.
                (Some(binary_len), None) if binary_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NChar(new_param) => match (old_param, new_param) {
                // NChar uses twice the space per length unit.
                (Some(binary_len), Some(nchar_len)) if binary_len > (nchar_len * 2) => RiskyCast,
                // By default we use two bytes.
                (Some(binary_len), None) if binary_len > 2 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(new_param) => match (old_param, new_param) {
                (Some(binary_len), Some(Number(varchar_len))) if binary_len > varchar_len.into() => RiskyCast,
                // By default we can fit one byte.
                (Some(binary_len), None) if binary_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                // NVarChar takes double the space per length unit.
                (Some(binary_len), Some(Number(nvarchar_len))) if binary_len > (nvarchar_len * 2).into() => RiskyCast,
                // By default we can fit two bytes.
                (Some(binary_len), None) if binary_len > 2 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Binary(new_param) => match (old_param, new_param) {
                (Some(old_len), Some(new_len)) if old_len > new_len => RiskyCast,
                // By default we can fit one byte.
                (Some(old_len), None) if old_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarBinary(new_param) => match (old_param, new_param) {
                (Some(old_len), Some(Number(new_len))) if old_len > new_len.into() => RiskyCast,
                // By default we can fit one byte.
                (Some(old_len), None) if old_len > 1 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Image => SafeCast,
            MsSqlType::Xml => RiskyCast,
            MsSqlType::UniqueIdentifier => RiskyCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::VarBinary(old_param)), Some(new_type)) => match new_type {
            MsSqlType::TinyInt => match old_param {
                // One byte.
                Some(Number(binary_len)) if binary_len <= 1 => SafeCast,
                None => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::SmallInt => match old_param {
                // Two bytes.
                Some(Number(binary_len)) if binary_len <= 2 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Int => match old_param {
                // Four bytes.
                Some(Number(binary_len)) if binary_len <= 4 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::BigInt => match old_param {
                // Eight bytes.
                Some(Number(binary_len)) if binary_len <= 8 => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Decimal(new_params) | MsSqlType::Numeric(new_params) => match (old_param, new_params) {
                // If precision is 9 or less, we can fit at most four bytes.
                (Some(Number(binary_len)), Some((p, _))) if p <= 9 && binary_len > 4 => RiskyCast,
                // If precision is 19 or less, we can fit at most eight bytes.
                (Some(Number(binary_len)), Some((p, _))) if p <= 19 && binary_len > 8 => RiskyCast,
                // If precision is 28 or less, we can fit at most twelve bytes.
                (Some(Number(binary_len)), Some((p, _))) if p <= 28 && binary_len > 12 => RiskyCast,
                // If precision is 38 or less, we can fit at most sixteen bytes.
                (Some(Number(binary_len)), Some((p, _))) if p <= 38 && binary_len > 16 => RiskyCast,
                // Default precision is 18, so we can have at most eight bytes.
                (Some(Number(binary_len)), None) if binary_len > 8 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Money => match old_param {
                // Spending eight bytes for money.
                Some(Number(binary_len)) if binary_len > 8 => RiskyCast,
                Some(Max) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::SmallMoney => match old_param {
                // Four bytes for money.
                Some(Number(binary_len)) if binary_len > 4 => RiskyCast,
                Some(Max) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Bit => RiskyCast,
            MsSqlType::Float(new_param) => match (old_param, new_param) {
                // Float takes four bytes.
                (Some(Number(binary_len)), Some(float_len)) if float_len <= 24 && binary_len > 4 => RiskyCast,
                // Double takes eight bytes.
                (Some(Number(binary_len)), Some(float_len)) if float_len <= 53 && binary_len > 8 => RiskyCast,
                // Defaults to double.
                (Some(Number(binary_len)), None) if binary_len > 8 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Real => match old_param {
                // Real is a float is four byes.
                Some(Number(binary_len)) if binary_len > 4 => RiskyCast,
                Some(Max) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::DateTime => RiskyCast,
            MsSqlType::SmallDateTime => RiskyCast,
            MsSqlType::Char(new_param) => match (old_param, new_param) {
                (Some(Number(binary_len)), Some(char_len)) if u32::from(binary_len) > char_len => RiskyCast,
                (Some(Number(binary_len)), None) if binary_len > 1 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NChar(new_param) => match (old_param, new_param) {
                // NChar length unit is two bytes.
                (Some(Number(binary_len)), Some(nchar_len)) if u32::from(binary_len) > (nchar_len * 2) => RiskyCast,
                // One nchar takes two bytes.
                (Some(Number(binary_len)), None) if binary_len > 2 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(new_param) => match (old_param, new_param) {
                (Some(Number(binary_len)), Some(Number(varchar_len))) if binary_len > varchar_len => RiskyCast,
                (Some(Number(binary_len)), None) if binary_len > 1 => RiskyCast,
                (Some(Max), Some(Max)) => SafeCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NVarChar(new_param) => match (old_param, new_param) {
                // NVarChar length unit is two bytes.
                (Some(Number(binary_len)), Some(Number(nvarchar_len))) if binary_len > (nvarchar_len * 2) => RiskyCast,
                // One nvarchar takes two bytes.
                (Some(Number(binary_len)), None) if binary_len > 2 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Binary(new_param) => match (old_param, new_param) {
                (Some(Number(old_len)), Some(new_len)) if u32::from(old_len) > new_len => RiskyCast,
                (Some(Number(old_len)), None) if old_len > 1 => RiskyCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarBinary(new_param) => match (old_param, new_param) {
                (Some(Number(old_len)), Some(Number(new_len))) if old_len > new_len => RiskyCast,
                (Some(Number(old_len)), None) if old_len > 1 => RiskyCast,
                (Some(Max), Some(Max)) => SafeCast,
                (Some(Max), _) => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Image => SafeCast,
            MsSqlType::Xml => RiskyCast,
            MsSqlType::UniqueIdentifier => RiskyCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::Image), Some(new_type)) => match new_type {
            MsSqlType::Binary(_) => RiskyCast,
            MsSqlType::VarBinary(param) => match param {
                Some(Max) => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Image => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::Xml), Some(new_type)) => match new_type {
            MsSqlType::Char(_) => RiskyCast,
            MsSqlType::NChar(_) => RiskyCast,
            // We might lose some information if VarChar is not using UTF-8
            // collation.
            MsSqlType::VarChar(_) => RiskyCast,
            MsSqlType::NVarChar(param) => match param {
                Some(Max) => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Binary(_) => RiskyCast,
            MsSqlType::VarBinary(param) => match param {
                Some(Max) => SafeCast,
                _ => RiskyCast,
            },
            MsSqlType::Xml => SafeCast,
            _ => NotCastable,
        },

        (Some(MsSqlType::UniqueIdentifier), Some(new_type)) => match new_type {
            MsSqlType::Char(param) => match param {
                // UUID is 36 characters.
                Some(char_len) if char_len < 36 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NChar(param) => match param {
                // UUID is 36 characters.
                Some(char_len) if char_len < 36 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarChar(param) => match param {
                // UUID is 36 characters.
                Some(Number(char_len)) if char_len < 36 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::NVarChar(param) => match param {
                // UUID is 36 characters.
                Some(Number(char_len)) if char_len < 36 => RiskyCast,
                None => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::Binary(param) => match param {
                // UUID is 16 bytes.
                Some(char_len) if char_len < 16 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::VarBinary(param) => match param {
                // UUID is 16 bytes.
                Some(Number(char_len)) if char_len < 16 => RiskyCast,
                _ => SafeCast,
            },
            MsSqlType::UniqueIdentifier => SafeCast,
            _ => NotCastable,
        },
    }
}
