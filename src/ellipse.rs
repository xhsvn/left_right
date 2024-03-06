use crate::distance_utils::{get_distance, R};

pub(crate) struct Ellipse {
    sin_o_lat: f64,
    cos_o_lat: f64,
    sin_o_lon: f64,
    cos_o_lon: f64,
    sin_d_lat: f64,
    cos_d_lat: f64,
    sin_d_lon: f64,
    cos_d_lon: f64,
    distance: f64,
    too_small: bool,
    two_alpha: f64,
}

impl Ellipse {
    pub(crate) fn new(o_lat: f64, o_lon: f64, d_lat: f64, d_lon: f64) -> Ellipse {
        let dis = get_distance(o_lat, o_lon, d_lat, d_lon);
        Ellipse {
            sin_o_lat: o_lat.sin(),
            cos_o_lat: o_lat.cos(),
            sin_o_lon: o_lon.sin(),
            cos_o_lon: o_lon.cos(),
            sin_d_lat: d_lat.sin(),
            cos_d_lat: d_lat.cos(),
            sin_d_lon: d_lon.sin(),
            cos_d_lon: d_lon.cos(),
            distance: dis,
            too_small: dis < 0.0001,
            two_alpha: 1.5 * dis,
        }
    }

    pub(crate) fn get_distance_from_origin(&self, c: &[f64; 2]) -> f64 {
        R * ((c[1].sin() * self.sin_o_lat) + c[1].cos() * self.cos_o_lat * (c[0].cos() * self.cos_o_lon + c[0].sin() * self.sin_o_lon)).acos()
    }

    pub(crate) fn get_distance_from_destination(&self, c: &[f64; 2]) -> f64 {
        R * ((c[1].sin() * self.sin_d_lat) + c[1].cos() * self.cos_d_lat * (c[0].cos() * self.cos_d_lon + c[0].sin() * self.sin_d_lon)).acos()
    }


    pub(crate) fn contains(&self, coord: &[f64; 2]) -> (bool, f64, f64) {
        let distance_from_origin = self.get_distance_from_origin(coord);
        let distance_from_destination = self.get_distance_from_destination(coord);
        let cont = distance_from_origin + distance_from_destination <= self.two_alpha;
        if self.too_small { (cont, 0.0, 0.0) } else { (cont, distance_from_origin, distance_from_destination) }
    }
}