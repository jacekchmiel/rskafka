use nom::{
    bytes::complete::take,
    combinator::{map, map_res},
    multi::many_m_n,
    IResult,
};
use rskafka_wire_format::{error::ParseError, parse_helpers, prelude::*, VarInt};
use std::borrow::Cow;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordBatch<'a> {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Cow<'a, [Record<'a>]>,
}

impl RecordBatch<'_> {
    pub fn detach(self) -> RecordBatch<'static> {
        RecordBatch {
            base_offset: self.base_offset,
            batch_length: self.batch_length,
            partition_leader_epoch: self.partition_leader_epoch,
            magic: self.magic,
            crc: self.crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            first_timestamp: self.first_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records: Cow::Owned(
                self.records
                    .into_owned()
                    .into_iter()
                    .map(Record::detach)
                    .collect(),
            ),
        }
    }
}

impl<'a> WireFormatBorrowParse<'a> for RecordBatch<'a> {
    fn borrow_parse(input: &'a [u8]) -> IResult<&'a [u8], Self, ParseError> {
        let (input, base_offset) = i64::parse(input)?;
        let (input, batch_length) = i32::parse(input)?;
        let (input, partition_leader_epoch) = i32::parse(input)?;
        let (input, magic) = i8::parse(input)?;
        let (input, crc) = u32::parse(input)?;
        let (input, attributes) = i16::parse(input)?;
        let (input, last_offset_delta) = i32::parse(input)?;
        let (input, first_timestamp) = i64::parse(input)?;
        let (input, max_timestamp) = i64::parse(input)?;
        let (input, producer_id) = i64::parse(input)?;
        let (input, producer_epoch) = i16::parse(input)?;
        let (input, base_sequence) = i32::parse(input)?;
        let (input, records) = map(Vec::<Record>::borrow_parse, Cow::Owned)(input)?;

        let batch = RecordBatch {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        };

        Ok((input, batch))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Record<'a> {
    pub length: VarInt,
    pub attributes: i8,
    pub timestamp_delta: VarInt,
    pub offset_delta: VarInt,
    pub key: Option<Cow<'a, [u8]>>,
    pub value: Option<Cow<'a, [u8]>>,
    pub headers: Cow<'a, [Header<'a>]>,
}

impl Record<'_> {
    pub fn detach(self) -> Record<'static> {
        Record {
            length: self.length,
            attributes: self.attributes,
            timestamp_delta: self.timestamp_delta,
            offset_delta: self.offset_delta,
            key: self.key.map(|key| Cow::Owned(key.into_owned())),
            value: self.value.map(|value| Cow::Owned(value.into_owned())),
            headers: Cow::Owned(
                self.headers
                    .into_owned()
                    .into_iter()
                    .map(Header::detach)
                    .collect(),
            ),
        }
    }
}

impl<'a> WireFormatBorrowParse<'a> for Record<'a> {
    fn borrow_parse(input: &'a [u8]) -> IResult<&'a [u8], Self, ParseError> {
        let (input, length) = VarInt::parse(input)?;
        let (input, attributes) = i8::parse(input)?;
        let (input, timestamp_delta) = VarInt::parse(input)?;
        let (input, offset_delta) = VarInt::parse(input)?;

        let (input, key_len) = map_res(VarInt::parse, parse_helpers::varint_as_opt_usize)(input)?;
        let (input, key) = match key_len {
            Some(len) => map(take(len), |v| Some(Cow::Borrowed(v)))(input)?,
            None => (input, None),
        };
        let (input, value_len) = map_res(VarInt::parse, parse_helpers::varint_as_opt_usize)(input)?;
        let (input, value) = match value_len {
            Some(len) => map(take(len), |v| Some(Cow::Borrowed(v)))(input)?,
            None => (input, None),
        };
        let (input, headers_cnt) = map_res(VarInt::parse, parse_helpers::int_as_usize)(input)?;
        let (input, headers) = map(
            many_m_n(headers_cnt, headers_cnt, Header::borrow_parse),
            Cow::Owned,
        )(input)?;

        let record = Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        };

        Ok((input, record))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Header<'a> {
    pub key: Cow<'a, str>,
    pub value: Cow<'a, [u8]>,
}

impl<'a> Header<'a> {
    pub fn detach(self) -> Header<'static> {
        Header {
            key: Cow::Owned(self.key.into_owned()),
            value: Cow::Owned(self.value.into_owned()),
        }
    }
}

impl<'a> WireFormatBorrowParse<'a> for Header<'a> {
    fn borrow_parse(input: &'a [u8]) -> IResult<&'a [u8], Self, ParseError> {
        let (input, key_len) = map_res(VarInt::parse, parse_helpers::int_as_usize)(input)?;
        let (input, key) = map(
            map_res(take(key_len), parse_helpers::bytes_as_utf8),
            Cow::Borrowed,
        )(input)?;
        let (input, val_len) = map_res(VarInt::parse, parse_helpers::int_as_usize)(input)?;
        let (input, value) = map(take(val_len), Cow::Borrowed)(input)?;

        Ok((input, Header { key, value }))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn header_over_bytes() {
        let bytes = vec![
            0x0e, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x32, 0x08, 0x61, 0x62, 0x63, 0x64,
        ];

        let header = Header::over_wire_bytes(&bytes).unwrap();
        assert_eq!(header.key, "header2");
        assert_eq!(header.value, Cow::Borrowed(&[0x61, 0x62, 0x63, 0x64]));
    }

    #[test]
    fn record_over_bytes() {
        let expected = Record {
            attributes: 0,
            length: VarInt(51),
            timestamp_delta: VarInt(0),
            offset_delta: VarInt(0),
            key: Some(Cow::Borrowed(&[
                0x64, 0x75, 0x70, 0x61, 0x2d, 0x6b, 0x65, 0x79,
            ])),
            value: Some(Cow::Borrowed(&[
                0x64, 0x75, 0x70, 0x61, 0x2d, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
            ])),
            headers: Cow::Borrowed(&[
                Header {
                    key: Cow::Borrowed("hader1"),
                    value: Cow::Borrowed(&[0x31, 0x32, 0x33, 0x34]),
                },
                Header {
                    key: Cow::Borrowed("header2"),
                    value: Cow::Borrowed(&[0x61, 0x62, 0x63, 0x64]),
                },
            ]),
        };

        let bytes = vec![
            0x66, 0x00, 0x00, 0x00, 0x10, 0x64, 0x75, 0x70, 0x61, 0x2d, 0x6b, 0x65, 0x79, 0x18,
            0x64, 0x75, 0x70, 0x61, 0x2d, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x04, 0x0c,
            0x68, 0x61, 0x64, 0x65, 0x72, 0x31, 0x08, 0x31, 0x32, 0x33, 0x34, 0x0e, 0x68, 0x65,
            0x61, 0x64, 0x65, 0x72, 0x32, 0x08, 0x61, 0x62, 0x63, 0x64,
        ];
        let record = Record::over_wire_bytes(&bytes).unwrap();
        assert_eq!(record, expected);
    }

    #[test]
    fn record_over_bytes_with_null_key() {
        let bytes = vec![20, 0, 0, 0, 1, 8, 100, 117, 112, 97, 0];
        let record = Record::over_wire_bytes(&bytes).unwrap();
        let expected = Record {
            length: VarInt(10),
            attributes: 0,
            timestamp_delta: VarInt(0),
            offset_delta: VarInt(0),
            key: None,
            value: Some(vec![100, 117, 112, 97].into()),
            headers: vec![].into(),
        };
        assert_eq!(record, expected);
    }

    #[test]
    fn record_batch_over_bytes() {
        let expected = RecordBatch {
            base_offset: 1,
            batch_length: 101,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0xa25f84b1,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0x00000171_ebdfc705,
            max_timestamp: 0x00000171_ebdfc705,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Cow::Borrowed(&[Record {
                attributes: 0,
                length: VarInt(51),
                timestamp_delta: VarInt(0),
                offset_delta: VarInt(0),
                key: Some(Cow::Borrowed(&[
                    0x64, 0x75, 0x70, 0x61, 0x2d, 0x6b, 0x65, 0x79,
                ])),
                value: Some(Cow::Borrowed(&[
                    0x64, 0x75, 0x70, 0x61, 0x2d, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
                ])),
                headers: Cow::Borrowed(&[
                    Header {
                        key: Cow::Borrowed("hader1"),
                        value: Cow::Borrowed(&[0x31, 0x32, 0x33, 0x34]),
                    },
                    Header {
                        key: Cow::Borrowed("header2"),
                        value: Cow::Borrowed(&[0x61, 0x62, 0x63, 0x64]),
                    },
                ]),
            }]),
        };

        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00,
            0x00, 0x00, 0x02, 0xa2, 0x5f, 0x84, 0xb1, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x71, 0xeb, 0xdf, 0xc7, 0x05, 0x00, 0x00, 0x01, 0x71, 0xeb, 0xdf, 0xc7,
            0x05, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00, 0x01, 0x66, 0x00, 0x00, 0x00, 0x10, 0x64, 0x75, 0x70, 0x61,
            0x2d, 0x6b, 0x65, 0x79, 0x18, 0x64, 0x75, 0x70, 0x61, 0x2d, 0x70, 0x61, 0x79, 0x6c,
            0x6f, 0x61, 0x64, 0x04, 0x0c, 0x68, 0x61, 0x64, 0x65, 0x72, 0x31, 0x08, 0x31, 0x32,
            0x33, 0x34, 0x0e, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x32, 0x08, 0x61, 0x62, 0x63,
            0x64,
        ];
        let record = RecordBatch::over_wire_bytes(&bytes).unwrap();
        assert_eq!(record, expected);
    }
}
