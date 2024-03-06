
pub(crate) const R: f64 = 6371.0; // Radius of the earth in km

pub(crate) fn get_distance(lat1_radians: f64, lon1_radians: f64, lat2_radians: f64, lon2_radians: f64) -> f64 {
    let d_lat = lat2_radians - lat1_radians;
    let d_lon = lon2_radians - lon1_radians;
    let a = (d_lat / 2.0).sin() * (d_lat / 2.0).sin() + lat1_radians.cos() * lat2_radians.cos() * (d_lon / 2.0).sin() * (d_lon / 2.0).sin();
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    R * c
}