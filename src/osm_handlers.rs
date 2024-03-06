use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::result::Result;
use smartstring::alias::String;

pub(crate) struct OsmProfile {
    pub(crate) access_tag_hierarchy: Vec<String>,
    pub(crate) access_tag_black: HashSet<String>,
    pub(crate) speed : f64,
    pub(crate) surface_speed_factor: HashMap<String, f64>,
}

struct OsmData {
    // forward_access: Option<String>,
    // backward_access: Option<String>,
    // one_way: Option<String>,
    is_reverse_one_way: Option<bool>,
    is_forward_one_way: Option<bool>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum WayMode {
    InAccessible,
    Walk,
}

pub struct OsmResult {
    pub(crate) forward_mode: WayMode,
    pub(crate) backward_mode: WayMode,
    pub(crate) forward_speed: f64,
    pub(crate) backward_speed: f64,
}


fn is_tag_true(tag: &String) -> bool {
    tag == "yes" || tag == "true" || tag == "1"
}


fn get_forward_backward_by_set<'a>(way: &'a osmpbfreader::Way, keys: &'a Vec<String>) -> ( Option<String>, Option< String>) {
    let mut forward: Option<&String> = None;
    let mut backward: Option<&String> = None;
    for key in keys {
        if forward.is_none() {
            let tmp = way.tags.get(&*(key.to_string() + ":forward"));
            if tmp.is_some() {
                forward = tmp;
            }

        }
        if backward.is_none() {
            let tmp = way.tags.get(&*(key.to_string() + ":backward"));
            if tmp.is_some() {
                backward = tmp
            }
        }

        if forward.is_none() || backward.is_none() {
            let tag = way.tags.get(key);
            if tag.is_some() {
                if forward.is_none() {
                    forward = tag;
                }
                if backward.is_none() {
                    backward = tag;
                }
            }
        }

        if forward.is_some() && backward.is_some() {
            return (forward.cloned(), backward.cloned());
        }
    }

    (forward.cloned(), backward.cloned())
}



pub(crate) fn handle_way(way: &osmpbfreader::Way, profile: &OsmProfile) -> Result<OsmResult, ()> {

    let highway = way.tags.get("highway");
    let bridge = way.tags.get("bridge");
    let route = way.tags.get("route");
    let leisure = way.tags.get("leisure");
    let man_made = way.tags.get("man_made");
    let railway = way.tags.get("railway");
    let platform = way.tags.get("platform");
    let amenity = way.tags.get("amenity");
    let public_transport = way.tags.get("public_transport");

    let mut data = OsmData {
        is_reverse_one_way: None,
        is_forward_one_way: None,
    };

    if highway.is_none() && bridge.is_none() && route.is_none() && leisure.is_none() &&
        man_made.is_none() && railway.is_none() && platform.is_none() && amenity.is_none() &&
        public_transport.is_none() {
        return Err(());
    }

    // println!("pass 1");
    if way.tags.get("area").is_some() && is_tag_true(way.tags.get("area").unwrap()) {
        return Err(());
    }

    // println!("pass 2");
    if way.tags.get("toll").is_some() && is_tag_true(way.tags.get("toll").unwrap()) {
        return Err(());
    }
    // println!("pass 3");
    if way.tags.get("construction").is_some() {
        return Err(());
    }

    // println!("pass 4");
    if way.tags.get("impassable").is_some() && is_tag_true(way.tags.get("impassable").unwrap()) {
        return Err(());
    }

    // println!("pass 5");
    if way.tags.get("status").is_some() && way.tags.get("status").unwrap() == "impassable" {
        return Err(());
    }

    let mut result = OsmResult {
        forward_mode: WayMode::Walk,
        backward_mode: WayMode::Walk,
        forward_speed: profile.speed,
        backward_speed: profile.speed
    };

    let (forward, backward) = get_forward_backward_by_set(way, &profile.access_tag_hierarchy);


    if forward.is_some() && profile.access_tag_black.contains(&*forward.unwrap()) {
        result.forward_mode = WayMode::InAccessible;
    }

    if backward.is_some() && profile.access_tag_black.contains(&*backward.unwrap()) {
        result.backward_mode = WayMode::InAccessible;
    }


    let one_way = way.tags.get("oneway");

    if let Some(one_way) = one_way {
        if one_way == "-1" {
            data.is_reverse_one_way = Some(true);
            result.forward_mode = WayMode::InAccessible;
        } else if is_tag_true(one_way) {
            data.is_forward_one_way = Some(true);
            result.backward_mode = WayMode::InAccessible;
        }
    }

    // println!("pass 6");
    if result.forward_mode == WayMode::InAccessible && result.backward_mode == WayMode::InAccessible {
        return Err(());
    }

    // println!("pass 7");
    if let Some(surface) = way.tags.get("surface") {
        if let Some(factor) = profile.surface_speed_factor.get(surface) {
            result.forward_speed *= factor;
            result.backward_speed *= factor;
        }
    }



    Ok(result)

}