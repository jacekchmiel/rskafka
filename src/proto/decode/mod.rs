pub(crate) mod primitive;

use super::{
    apis::api_versions::ApiVersionsRange,
    data::{api_key::ApiKey, error::ErrorCode, header::ResponseHeader},
};
use nom::{
    combinator::{map, map_res},
    multi::many0,
    number::complete::{le_i16, le_i32},
    sequence::tuple,
    IResult,
};
use std::convert::TryFrom;

// RequestOrResponse => Size (RequestMessage | ResponseMessage)
//   Size => int32
fn size(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, size) = le_i32(input)?;
    // let (input, size_bytes) = take(size_of::<i32>())(input)?;
    // let size = i32::from_le_bytes(size_bytes.try_into().unwrap());
    // let (input, _) = tag("#")(input)?;
    // let (input, (red, green, blue)) = tuple((hex_primary, hex_primary, hex_primary))(input)?;

    // Ok((input, Color { red, green, blue }))
    Ok((input, usize::try_from(size).unwrap())) //FIXME: errorasd
}

// fn api_key(input: &[u8]) -> IResult<&[u8], ApiKey> {
//     map_res(le_i16, ApiKey::try_from_i16)(input)
// }

// fn api_key_fallible(input: &[u8]) -> IResult<&[u8], Option<ApiKey>> {
//     map(le_i16, |v| ApiKey::try_from_i16(v).ok())(input)
// }

// fn supported_api_versions(input: &[u8]) -> IResult<&[u8], SupportedApiVersions> {
//     map(
//         tuple((api_key, le_i16, le_i16)),
//         SupportedApiVersions::from_tuple,
//     )(input)
// }

// fn supported_api_versions_filtered_array(
//     input: &[u8],
// ) -> IResult<&[u8], Vec<SupportedApiVersions>> {
//     map(many0(supported_api_versions), |versions| {
//         versions.into_iter().filter_map(|v| v).collect()
//     })(input)
// }

// fn api_versions_response_v0(input: &[u8]) -> IResult<&[u8], ApiVersionsResponse> {
//     map(
//         tuple((error_code, supported_api_versions_filtered_array)),
//         ApiVersionsResponse::from_tuple,
//     )(input)
// }

// pub(crate) fn response(input: &[u8]) -> IResult<&[u8], Response> {
//     let (input, header) = response_header(&input)?;

//     let (input, response_kind) = match (header.request_api_key, header.request_api_version) {
//         (ApiKey::ApiVersions, 0) => map(api_versions_response_v0, ResponseKind::from)(input)?,
//         (_, _) => panic!("Unsupported message"), //FIXME: error
//     };

//     Ok((
//         input,
//         Response {
//             correlation_id: header.correlation_id,
//             kind: response_kind,
//         },
//     ))
// }

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_size() {
        let (rem, size) = size(&[0x01, 0x00, 0x00, 0x00, 0x05]).unwrap();
        assert_eq!(rem, &[0x05]);
        assert_eq!(size, 1);
    }
}
