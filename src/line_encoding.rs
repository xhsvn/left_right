pub(crate) fn encode_to_byte_array(coords : &Vec<[i32; 2]>) -> Vec<u8> {
    let mut result = Vec::new();
    let mut prev_lat = 0;
    let mut prev_lon = 0;
    for coord in coords {
        let lat = coord[1];
        let lon = coord[0];
        let d_lat = lat - prev_lat;
        let d_lon = lon - prev_lon;
        let mut d_lat = (d_lat << 1) ^ (d_lat >> 31);
        let mut d_lon = (d_lon << 1) ^ (d_lon >> 31);
        let mut index = ((d_lon + d_lat) * (d_lon + d_lat + 1) / 2) + d_lat;
        while index > 0 {
            let mut byte = (index & 0x1F) as u8;
            index >>= 5;
            if index > 0 {
                byte |= 0x20;
            }
            result.push(byte);
        }

        prev_lat = lat;
        prev_lon = lon;
    }
    result
}

pub(crate) fn decode_from_byte_array(encoded : &Vec<u8>) -> Vec<[i32; 2]> {
    let mut result = Vec::new();
    let mut prev_lat = 0;
    let mut prev_lon = 0;
    let mut i = 0;
    while i < encoded.len() {
        let mut byte = encoded[i] as i32;
        let mut lat = 0;
        let mut lon = 0;
        let mut shift = 0;
        while byte >= 0x20 {
            byte -= 0x20;
            let next_byte = encoded[i + 1] as i32;
            i += 1;
            lat += (next_byte & 0x01) << shift;
            lon += (next_byte >> 1) << shift;
            shift += 5;
        }
        lat += byte << shift;
        lon += ((encoded[i + 1] as i32) >> 1) << shift;
        lat = (lat >> 1) ^ -(lat & 1);
        lon = (lon >> 1) ^ -(lon & 1);
        prev_lat += lat;
        prev_lon += lon;
        result.push([prev_lon, prev_lat]);
        i += 1;
    }
    result
}
