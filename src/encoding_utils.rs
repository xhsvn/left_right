

pub(crate) fn encode_to_mid_point_id(way_id: i64, first_index: usize, second_index: usize) -> u64 {
    ((way_id as u64) << 24) + ((first_index as u64) << 12) + (second_index as u64)
}

pub(crate) fn decode_mid_point_id(id: u64) -> (i64, usize, usize) {
    ((id >> 24) as i64, ((id >> 12) & 0xFFF) as usize, (id & 0xFFF) as usize)
}


pub(crate) fn get_cross_data_key(src: u64, dst: u64) -> u128 {
    ((src as u128) << 64) | (dst as u128)
}