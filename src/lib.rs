#![feature(trait_alias)]
use const_format::concatcp;
use nom::branch::alt;
use nom::bytes::complete::{escaped, tag};
use nom::character::complete::{char, digit1, none_of, one_of};
use nom::combinator::{map, opt};
use nom::error::Error;
use nom::multi::{many0, many1};
use nom::number::complete::double;
use nom::sequence::{delimited, separated_pair, terminated, tuple};
use nom::Parser as NomParser;
use std::str::FromStr;

trait Parser<'a, T> = NomParser<&'a str, T, Error<&'a str>>;

///
/// Syntax
/// <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
///
/// Example:
/// myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000
///

#[derive(Debug, Eq, PartialEq)]
pub struct Tag<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Debug, PartialEq)]
pub enum FieldValue<'a> {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    String(&'a str),
    Boolean(bool),
}

#[derive(Debug, PartialEq)]
pub struct Field<'a> {
    key: &'a str,
    value: FieldValue<'a>,
}

#[derive(Debug, PartialEq)]
pub struct Measurement<'a> {
    name: &'a str,
    tags: Vec<Tag<'a>>,
    fields: Vec<Field<'a>>,
    timestamp: Option<i64>,
}

const BASE_EXCLUDED_CHARS: &str = "\t\n\r";
const CONTROL_CHARS: &str = "\\";

// See: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters
const MEASUREMENT_SPECIAL_CHARS: &str = ", ";
const TAG_KEY_SPECIAL_CHARS: &str = ",= ";
const TAG_VALUE_SPECIAL_CHARS: &str = ",= ";
const FIELD_KEY_SPECIAL_CHARS: &str = ",= ";
const FIELD_VALUE_SPECIAL_CHARS: &str = r#""\"#;

fn parse_measurement_naive<'a>() -> impl Parser<'a, &'a str> {
    escaped(
        none_of(concatcp!(
            BASE_EXCLUDED_CHARS,
            CONTROL_CHARS,
            MEASUREMENT_SPECIAL_CHARS
        )),
        '\\',
        one_of(", "),
    )
}

fn parse_tag<'a>() -> impl Parser<'a, Tag<'a>> {
    map(
        separated_pair(
            escaped(
                none_of(concatcp!(
                    BASE_EXCLUDED_CHARS,
                    CONTROL_CHARS,
                    TAG_KEY_SPECIAL_CHARS
                )),
                '\\',
                one_of(",= "),
            ),
            char('='),
            escaped(
                none_of(concatcp!(
                    BASE_EXCLUDED_CHARS,
                    CONTROL_CHARS,
                    TAG_VALUE_SPECIAL_CHARS
                )),
                '\\',
                one_of(",= "),
            ),
        ),
        |(t, v)| Tag { key: t, value: v },
    )
}

fn parse_tags<'a>() -> impl Parser<'a, Vec<Tag<'a>>> {
    many0(alt(((terminated(parse_tag(), char(','))), parse_tag())))
}

fn parse_measurement_and_tags<'a>() -> impl Parser<'a, (&'a str, Vec<Tag<'a>>)> {
    alt((
        separated_pair(parse_measurement_naive(), char(','), parse_tags()),
        parse_measurement_naive().map(|measurement| (measurement, vec![])),
    ))
}

fn parse_string_field_value<'a>() -> impl Parser<'a, FieldValue<'a>> {
    delimited(
        char('"'),
        escaped(
            none_of(concatcp!(
                BASE_EXCLUDED_CHARS,
                CONTROL_CHARS,
                FIELD_VALUE_SPECIAL_CHARS
            )),
            '\\',
            one_of(r#""\"#),
        ),
        char('"'),
    )
    .map(|v| FieldValue::String(v))
}

fn parse_boolean_field_value<'a>() -> impl Parser<'a, FieldValue<'a>> {
    alt((
        alt((tag("true"), tag("t"), tag("TRUE"), tag("True"), tag("T")))
            .map(|_| FieldValue::Boolean(true)),
        alt((tag("false"), tag("f"), tag("FALSE"), tag("False"), tag("F")))
            .map(|_| FieldValue::Boolean(false)),
    ))
}

fn parse_i64<'a>() -> impl Parser<'a, i64> {
    tuple((opt(char('-')), digit1)).map(|(sign, digits)| {
        let sign: String = sign.map(|_| String::from("-")).unwrap_or(String::new());
        // TODO: unwrap
        i64::from_str(&(sign + digits)).unwrap()
    })
}

fn parse_integer_field_value<'a>() -> impl Parser<'a, FieldValue<'a>> {
    terminated(parse_i64(), char('i')).map(FieldValue::Integer)
}

fn parse_uinteger_field_value<'a>() -> impl Parser<'a, FieldValue<'a>> {
    terminated(digit1, char('u')).map(|digits| {
        // TODO: unwrap
        FieldValue::UInteger(u64::from_str(digits).unwrap())
    })
}

fn parse_float_field_value<'a>() -> impl Parser<'a, FieldValue<'a>> {
    double.map(FieldValue::Float)
}

fn parse_field_value<'a>() -> impl Parser<'a, FieldValue<'a>> {
    alt((
        parse_string_field_value(),
        parse_boolean_field_value(),
        parse_uinteger_field_value(),
        parse_integer_field_value(),
        parse_float_field_value(),
    ))
}

fn parse_field<'a>() -> impl Parser<'a, Field<'a>> {
    separated_pair(
        escaped(
            none_of(concatcp!(
                BASE_EXCLUDED_CHARS,
                CONTROL_CHARS,
                FIELD_KEY_SPECIAL_CHARS
            )),
            '\\',
            one_of(r#",= "#),
        ),
        char('='),
        parse_field_value(),
    )
    .map(|(f, v)| Field { key: f, value: v })
}

fn parse_fields<'a>() -> impl Parser<'a, Vec<Field<'a>>> {
    many1(alt((terminated(parse_field(), char(',')), parse_field())))
}

fn parse_timestamp<'a>() -> impl Parser<'a, i64> {
    parse_i64()
}

fn parse_ilp_line<'a>() -> impl Parser<'a, Measurement<'a>> {
    tuple((
        parse_measurement_and_tags(),
        char(' '),
        parse_fields(),
        opt(tuple((char(' '), parse_timestamp()))),
    ))
    .map(|((measurement, tags), _, fields, maybe_timestamp)| {
        let timestamp = maybe_timestamp.map(|(_, ts)| ts);
        Measurement {
            name: measurement,
            tags,
            fields,
            timestamp,
        }
    })
}

fn parse_ilp_lines<'a>() -> impl Parser<'a, Vec<Measurement<'a>>> {
    // TODO: does each line need EOL in order to be valid?
    many1(terminated(parse_ilp_line(), char('\n')))
}

pub fn parse(value: &str) -> Vec<Measurement> {
    let (_rest, result) = parse_ilp_lines().parse(value).unwrap();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    mod measurement {
        use super::*;

        #[test]
        fn parse_measurement_simple_string() {
            let (_rest, result) = parse_measurement_naive().parse("myMeasurement").unwrap();
            assert_eq!(result, "myMeasurement");
        }

        #[test]
        fn parse_measurement_with_escape_chars() {
            let (_rest, result) = parse_measurement_naive()
                .parse(r#"my\,\ Measurement"#)
                .unwrap();
            assert_eq!(result, r#"my\,\ Measurement"#);
        }
    }

    mod tags {
        use super::*;

        #[test]
        fn parse_tag_parses_tag() {
            let (_rest, result) = parse_tag().parse("foo=bar").unwrap();
            assert_eq!(
                result,
                Tag {
                    key: "foo",
                    value: "bar"
                }
            );
        }

        #[test]
        fn parse_tags_parses_one_tag() {
            let (_rest, result) = parse_tags().parse("foo=bar").unwrap();
            assert_eq!(
                result,
                vec![Tag {
                    key: "foo",
                    value: "bar"
                }]
            );
        }

        #[test]
        fn parse_tags_parses_two_tags() {
            let (_rest, result) = parse_tags().parse("foo=bar,baz=buzz").unwrap();
            assert_eq!(
                result,
                vec![
                    Tag {
                        key: "foo",
                        value: "bar"
                    },
                    Tag {
                        key: "baz",
                        value: "buzz"
                    },
                ]
            );
        }

        #[test]
        fn parse_tags_parses_tag_key_with_escaped_chars() {
            let (_rest, result) = parse_tags().parse(r#"a\,\=\ foo=bar"#).unwrap();
            assert_eq!(
                result,
                vec![Tag {
                    key: r#"a\,\=\ foo"#,
                    value: "bar"
                }]
            );
        }

        #[test]
        fn parse_tags_parses_tag_value_with_escaped_chars() {
            let (_rest, result) = parse_tags().parse(r#"foo=a\,\=\ bar"#).unwrap();
            assert_eq!(
                result,
                vec![Tag {
                    key: "foo",
                    value: r#"a\,\=\ bar"#
                }]
            );
        }
    }

    #[test]
    fn parse_measurement_and_tags_with_one_tag_parses() {
        let (_rest, result) = parse_measurement_and_tags()
            .parse("myMeasurement,foo=bar")
            .unwrap();
        assert_eq!(
            result,
            (
                "myMeasurement",
                vec![Tag {
                    key: "foo",
                    value: "bar"
                }]
            )
        );
    }

    #[test]
    fn parse_measurement_and_tags_with_no_tag_parses() {
        let (_rest, result) = parse_measurement_and_tags().parse("myMeasurement").unwrap();
        assert_eq!(result, ("myMeasurement", vec![]));
    }

    #[test]
    fn parse_measurement_and_tags_with_many_tags_parses() {
        let (_rest, result) = parse_measurement_and_tags()
            .parse("myMeasurement,foo=bar,baz=bur,bling=blang")
            .unwrap();
        assert_eq!(
            result,
            (
                "myMeasurement",
                vec![
                    Tag {
                        key: "foo",
                        value: "bar"
                    },
                    Tag {
                        key: "baz",
                        value: "bur"
                    },
                    Tag {
                        key: "bling",
                        value: "blang"
                    },
                ]
            )
        );
    }

    mod field {
        #![allow(non_snake_case)]
        use super::*;

        #[test]
        fn parse_string_field_parses() {
            let (_rest, result) = parse_field().parse("foo=\"bar\"").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::String("bar")
                }
            );
        }

        #[test]
        fn parse_string_field_key_with_escaped_chars_parses() {
            let (_rest, result) = parse_field().parse(r#"bax\,\=\ foo="bar""#).unwrap();
            assert_eq!(
                result,
                Field {
                    key: r#"bax\,\=\ foo"#,
                    value: FieldValue::String("bar")
                }
            );
        }

        #[test]
        fn parse_string_field_value_with_escaped_chars_parses() {
            let (_rest, result) = parse_field().parse(r#"foo="\"\\bar""#).unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::String(r#"\"\\bar"#)
                }
            );
        }

        #[test]
        fn parse_decimal_float_field_parses() {
            let (_rest, result) = parse_field().parse("foo=1.0").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Float(1.0f64)
                }
            );
        }

        #[test]
        fn parse_bare_float_field_parses() {
            let (_rest, result) = parse_field().parse("foo=1").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Float(1.0f64)
                }
            );
        }

        #[test]
        fn parse_negative_bare_float_field_parses() {
            let (_rest, result) = parse_field().parse("foo=-1").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Float(-1.0f64)
                }
            );
        }

        #[test]
        fn parse_scientific_float_field_parses() {
            let (_rest, result) = parse_field().parse("foo=-1.234456e+78").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Float(-1.234456e+78f64)
                }
            );
        }

        #[test]
        fn parse_integer_field_parses() {
            let (_rest, result) = parse_field().parse("foo=1i").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Integer(1i64)
                }
            );
        }

        #[test]
        fn parse_negative_integer_field_parses() {
            let (_rest, result) = parse_field().parse("foo=-12485903i").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Integer(-12485903i64)
                }
            );
        }

        #[test]
        fn parse_max_integer_field_parses() {
            let (_rest, result) = parse_field().parse("foo=9223372036854775807i").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Integer(9223372036854775807i64)
                }
            );
        }

        #[test]
        fn parse_min_integer_field_parses() {
            let (_rest, result) = parse_field().parse("foo=-9223372036854775808i").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Integer(-9223372036854775808i64)
                }
            );
        }

        #[test]
        fn parse_uinteger_field_parses() {
            let (_rest, result) = parse_field().parse("foo=1u").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::UInteger(1u64)
                }
            );
        }

        #[test]
        fn parse_max_uinteger_field_parses() {
            let (_rest, result) = parse_field().parse("foo=18446744073709551615u").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::UInteger(18446744073709551615u64)
                }
            );
        }

        #[test]
        fn parse_boolean_true_field_parses() {
            let (_rest, result) = parse_field().parse("foo=true").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(true)
                }
            );
        }

        #[test]
        fn parse_boolean_false_field_parses() {
            let (_rest, result) = parse_field().parse("foo=false").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(false)
                }
            );
        }

        #[test]
        fn parse_boolean_True_field_parses() {
            let (_rest, result) = parse_field().parse("foo=True").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(true)
                }
            );
        }

        #[test]
        fn parse_boolean_False_field_parses() {
            let (_rest, result) = parse_field().parse("foo=False").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(false)
                }
            );
        }

        #[test]
        fn parse_boolean_t_field_parses() {
            let (_rest, result) = parse_field().parse("foo=t").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(true)
                }
            );
        }

        #[test]
        fn parse_boolean_f_field_parses() {
            let (_rest, result) = parse_field().parse("foo=f").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(false)
                }
            );
        }

        #[test]
        fn parse_boolean_T_field_parses() {
            let (_rest, result) = parse_field().parse("foo=T").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(true)
                }
            );
        }

        #[test]
        fn parse_boolean_F_field_parses() {
            let (_rest, result) = parse_field().parse("foo=F").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(false)
                }
            );
        }

        #[test]
        fn parse_boolean_TRUE_field_parses() {
            let (_rest, result) = parse_field().parse("foo=TRUE").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(true)
                }
            );
        }

        #[test]
        fn parse_boolean_FALSE_field_parses() {
            let (_rest, result) = parse_field().parse("foo=FALSE").unwrap();
            assert_eq!(
                result,
                Field {
                    key: "foo",
                    value: FieldValue::Boolean(false)
                }
            );
        }
    }

    mod timestamp {
        use super::*;

        #[test]
        fn parse_min_timestamp() {
            let (_rest, result) = parse_timestamp().parse("-9223372036854775806").unwrap();
            assert_eq!(result, -9223372036854775806i64);
        }

        #[test]
        fn parse_max_timestamp() {
            let (_rest, result) = parse_timestamp().parse("9223372036854775806").unwrap();
            assert_eq!(result, 9223372036854775806);
        }
    }

    #[test]
    fn parse_ilp_line_parses() {
        let (_rest, result) = parse_ilp_line()
            .parse(
                "myMeasurement,tag1=value1,tag2=value2 fieldKey=\"fieldValue\" 1556813561098000000",
            )
            .unwrap();
        assert_eq!(
            result,
            Measurement {
                name: "myMeasurement",
                tags: vec![
                    Tag {
                        key: "tag1",
                        value: "value1"
                    },
                    Tag {
                        key: "tag2",
                        value: "value2"
                    }
                ],
                fields: vec![Field {
                    key: "fieldKey",
                    value: FieldValue::String("fieldValue")
                }],
                timestamp: Some(1556813561098000000i64)
            }
        )
    }

    #[test]
    fn parse_ilp_line_no_timestamp_parses() {
        let (_rest, result) = parse_ilp_line()
            .parse("myMeasurement fieldKey=\"fieldValue\"")
            .unwrap();
        assert_eq!(
            result,
            Measurement {
                name: "myMeasurement",
                tags: vec![],
                fields: vec![Field {
                    key: "fieldKey",
                    value: FieldValue::String("fieldValue")
                }],
                timestamp: None
            }
        )
    }

    #[test]
    fn parse_ilp_line_with_string_in_string_parses() {
        let (_rest, result) = parse_ilp_line()
            .parse(r#"myMeasurement fieldKey="\"string\" within a string""#)
            .unwrap();
        assert_eq!(
            result,
            Measurement {
                name: "myMeasurement",
                tags: vec![],
                fields: vec![Field {
                    key: "fieldKey",
                    value: FieldValue::String(r#"\"string\" within a string"#)
                }],
                timestamp: None
            }
        )
    }

    #[test]
    fn parse_ilp_spaces_in_tags_keys_line_parses() {
        let (_rest, result) = parse_ilp_line()
            .parse(r#"myMeasurement,tag\ Key1=tag\ Value1,tag\ Key2=tag\ Value2 fieldKey=100"#)
            .unwrap();
        assert_eq!(
            result,
            Measurement {
                name: "myMeasurement",
                tags: vec![
                    Tag {
                        key: r"tag\ Key1",
                        value: r"tag\ Value1"
                    },
                    Tag {
                        key: r"tag\ Key2",
                        value: r"tag\ Value2"
                    }
                ],
                fields: vec![Field {
                    key: "fieldKey",
                    value: FieldValue::Float(100f64)
                }],
                timestamp: None
            }
        )
    }

    #[test]
    fn parse_ilp_emoji_line_parses() {
        let (_rest, result) = parse_ilp_line()
            .parse("myMeasurement,tagKey=🍭 fieldKey=\"Launch 🚀\" 1556813561098000000")
            .unwrap();
        assert_eq!(
            result,
            Measurement {
                name: "myMeasurement",
                tags: vec![Tag {
                    key: "tagKey",
                    value: "🍭"
                }],
                fields: vec![Field {
                    key: "fieldKey",
                    value: FieldValue::String("Launch 🚀")
                }],
                timestamp: Some(1556813561098000000i64)
            }
        )
    }
}
