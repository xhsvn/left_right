
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use rtree_rs::{RTree, Rect};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, Read, Write};
use std::process::exit;
use smartstring::alias::String;
use crate::distance_utils::{get_distance};
use crate::encoding_utils::{encode_to_mid_point_id, get_cross_data_key};
use crate::osm_handlers::{handle_way, OsmProfile, OsmResult, WayMode};
use std::path::Path;
use std::u128;

const PRECISION_FLOAT: f64 = 10_000_000.0;


#[derive(PartialEq)]
pub(crate) struct GNode {
    id: i64,
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    // tags: flat_map::FlatMap<String, String>
}
//g Way with life time



pub(crate) struct GWay {
    id: i64,
    pub(crate) nodes: Vec<i64>,
    distances: Vec<f64>,
    // tags: ::flat_map::FlatMap<String, String>
    pub(crate) node_id_to_index: HashMap<i64, usize>,
    pub(crate) before_mid_points: Vec<usize>,
    pub(crate) after_mid_points: Vec<usize>,

}

pub(crate) struct GraphBuildInfo {
    pub(crate) way_id_to_result: HashMap<i64, OsmResult>,
    pub(crate) node_id_to_way_ids: HashMap<i64, HashSet<i64>>,
    pub(crate) nodes: HashMap<i64, GNode>,
    pub(crate) ways: HashMap<i64, GWay>,
}


#[derive(Debug)]
pub(crate) struct CrossData {
    pub(crate) start_mid_point_id: u64,
    pub(crate) end_mid_point_id: u64,
    pub(crate) distance: f64,
    pub(crate) duration: i32,
    pub(crate) start_forward: bool,
    pub(crate) end_forward: bool,
    pub(crate) center_mid_point_id: u64,
}

pub(crate) struct WayRoutingInfo {
    pub(crate) nodes_coord: Vec<[f64; 2]>,

}

pub(crate) struct PostOsmGraph {
    pub(crate) ways: HashMap<i64, WayRoutingInfo>,
    pub(crate) mid_point_id_to_coord: HashMap<u64, [f64; 2]>,
    pub(crate) cross_datas: HashMap<u128, CrossData>,
    pub(crate) cd_map: HashMap<u64, Vec<(u64, i32)>>,
    pub(crate) cd_map_reverse: HashMap<u64, Vec<(u64, i32)>>,
    pub(crate) rtree: RTree<2, f64, u64>,
    pub(crate) ranks: HashMap<u64, usize>,
}

pub(crate) fn encode_to_byte_array(graph: &PostOsmGraph) -> Vec<u8> {
    let mut buf : Vec<u8> = Vec::new();

    //first add len of ways
    let len = graph.ways.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());

    //then add ways
    for (way_id, way) in &graph.ways {
        //first add id
        buf.extend_from_slice(&way_id.to_le_bytes());
        // add len of nodes
        let len = way.nodes_coord.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        //add nodes
        for node in &way.nodes_coord {
            buf.extend_from_slice(&node[0].to_le_bytes());
            buf.extend_from_slice(&node[1].to_le_bytes());
        }
    }

    // add len of mid_point_id_to_coord
    let len = graph.mid_point_id_to_coord.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    for (mid_point_id, coord) in &graph.mid_point_id_to_coord {
        buf.extend_from_slice(&mid_point_id.to_le_bytes());
        buf.extend_from_slice(&coord[0].to_le_bytes());
        buf.extend_from_slice(&coord[1].to_le_bytes());
    }

    // add len of cross_dates
    let len = graph.cross_datas.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());

    let mut key_to_index = HashMap::new();
    for (idx, (key, cd)) in (&graph.cross_datas).into_iter().enumerate() {
        key_to_index.insert(key, idx);
        buf.extend_from_slice(&cd.start_mid_point_id.to_le_bytes());
        buf.extend_from_slice(&cd.end_mid_point_id.to_le_bytes());
        buf.extend_from_slice(&cd.distance.to_le_bytes());
        buf.extend_from_slice(&cd.duration.to_le_bytes());
        let direction = (cd.start_forward as u8) << 1 | (cd.end_forward as u8);
        buf.push(direction);
    }


    // add len of cd_map
    let len = graph.cd_map.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    for (src, dsts) in &graph.cd_map {
        buf.extend_from_slice(&src.to_le_bytes());
        buf.extend_from_slice(&(dsts.len() as u32).to_le_bytes());
        for (dst, _) in dsts {
            let index = key_to_index[&get_cross_data_key(*src, *dst)] as u32;
            buf.extend_from_slice(&dst.to_le_bytes());
            buf.extend_from_slice(&index.to_le_bytes());
        }
    }


    let len = graph.cd_map_reverse.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    for (dst, srcs) in &graph.cd_map_reverse {
        buf.extend_from_slice(&dst.to_le_bytes());
        buf.extend_from_slice(&(srcs.len() as u32).to_le_bytes());
        for (src, _) in srcs {
            let index = key_to_index[&get_cross_data_key(*src, *dst)] as u32;
            buf.extend_from_slice(&src.to_le_bytes());
            buf.extend_from_slice(&index.to_le_bytes());
        }
    }


    buf
}





impl PostOsmGraph {
    pub(crate) fn new() -> Self {
        PostOsmGraph {
            ways: HashMap::new(),
            mid_point_id_to_coord: HashMap::new(),
            cross_datas: HashMap::new(),
            cd_map: Default::default(),
            cd_map_reverse: Default::default(),
            rtree: RTree::new(),
            ranks: Default::default(),
        }
    }
}




pub(crate) fn load_hc(pbf_path: &Path) -> Vec<u128> {
    let path = pbf_path.parent().unwrap().join("hc.bin");
    let mut file = File::open(path.as_path());

    if file.is_err() {
        return Vec::new();
    }

    let mut file = file.unwrap();


    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();

    let mut dups = Vec::new();
    let mut index = 0;
    let dups_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
    index += 4;
    for _ in 0..dups_len {
        let d = u128::from_le_bytes(buf[index..index + 16].try_into().unwrap());
        index += 16;
        dups.push(d);
    }

    dups
}


pub(crate) fn save_graph(pbf_path: &Path, graph: &PostOsmGraph) {
    let vec_array = encode_to_byte_array(graph);
    let path = pbf_path.parent().unwrap().join("post_osm.bin");

    let mut file = File::create(path.as_path()).unwrap();
    file.write_all(&vec_array).unwrap();
}


//
pub(crate) fn read_pbf(pbf_path: &Path, graph: &mut PostOsmGraph, build_info: &mut GraphBuildInfo, profile: &OsmProfile) {
    let pbf = File::open(pbf_path);
    let pbf = match pbf {
        Ok(pbf) => pbf,
        Err(_) => {
            eprintln!("Error: Cannot open file {}", pbf_path.to_str().unwrap());
            exit(1);
        }
    };
    let mut pbf = osmpbfreader::OsmPbfReader::new(BufReader::new(pbf));
    let mut unprocessed_nodes: Vec<osmpbfreader::Node> = vec![];
    let mut unprocessed_ways: Vec<osmpbfreader::Way> = vec![];

    {
        for obj in pbf.par_iter() {
            let obj = obj.unwrap_or_else(|e| {
                println!("{:?}", e);
                exit(1)
            });
            match obj {
                osmpbfreader::OsmObj::Node(node) => {
                    unprocessed_nodes.push(node);
                }
                osmpbfreader::OsmObj::Way(way) => {
                    unprocessed_ways.push(way);
                }
                osmpbfreader::OsmObj::Relation(_) => {}
            }
        }
    }

    let mut mid_processed_ways: Vec<osmpbfreader::Way> = vec![];


    for way in unprocessed_ways {

        if way.nodes.len() < 2 {
            continue;
        }

        let result = handle_way(&way, profile);

        if result.is_err() {
            continue;
        }
        let result = result.unwrap();
        build_info.way_id_to_result.insert(way.id.0, result);

        let mut visited: HashSet<i64> = HashSet::new();

        for node_id in &way.nodes {
            let node_id = node_id.0;
            if visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id);
            let way_ids = build_info.node_id_to_way_ids.entry(node_id).or_insert(HashSet::new());
            way_ids.insert(way.id.0);
        }
        mid_processed_ways.push(way);
    }

    let mut ways_to_remove: Vec<osmpbfreader::Way> = vec![];
    let mut unprocessed_ways: Vec<osmpbfreader::Way> = vec![];
    for way in mid_processed_ways {

        let mut del = true;

        for node_id in &way.nodes {
            let node_id = node_id.0;
            if build_info.node_id_to_way_ids.get(&node_id).unwrap().len() > 1 {
                del = false;
                break;
            }
        }
        if del {
            ways_to_remove.push(way);
        } else {
            unprocessed_ways.push(way);
        }
    }

    let mut to_remove_nodes: HashSet<i64> = HashSet::new();

    for way in ways_to_remove {
        for node_id in &way.nodes {
            let node_id = node_id.0;
            let l = build_info.node_id_to_way_ids.get(&node_id).unwrap().len();
            if l == 1 {
                to_remove_nodes.insert(node_id);
            }
        }
    }





    for node in unprocessed_nodes {
        if to_remove_nodes.contains(&node.id.0) || !build_info.node_id_to_way_ids.contains_key(&node.id.0) {
            continue;
        }
        build_info.nodes.insert(node.id.0, GNode {
            id: node.id.0,
            lat: node.lat().to_radians(),
            lon: node.lon().to_radians(),
        });
    }


    for way in unprocessed_ways {
        let mut last_distance = 0.0;
        let mut g_way = GWay {
            id: way.id.0,
            nodes: way.nodes.iter().map(|n| n.0).collect(),
            distances: vec![],
            node_id_to_index: Default::default(),
            before_mid_points: vec![usize::MAX; way.nodes.len()],
            after_mid_points: vec![usize::MAX; way.nodes.len()],
        };
        let mut r_way = WayRoutingInfo {
            nodes_coord: vec![],
        };
        for i in 0..way.nodes.len() {
            let node_id = way.nodes[i];

            if i == 0 || i == (way.nodes.len() - 1) || build_info.node_id_to_way_ids.get(&node_id.0).unwrap().len() > 1 {
                g_way.node_id_to_index.insert(node_id.0, i);
            }
            let nn = build_info.nodes.get(&node_id.0).unwrap();
            r_way.nodes_coord.push([nn.lon, nn.lat]);

            if i > 0 {
                let last_node = build_info.nodes.get(&g_way.nodes[i - 1]).unwrap();
                let current_node = build_info.nodes.get(&g_way.nodes[i]).unwrap();
                let distance = get_distance(last_node.lat, last_node.lon, current_node.lat, current_node.lon);
                last_distance += distance * 1000.0;
                g_way.distances.push(last_distance);
            } else {
                g_way.distances.push(0.0);
            }
        }

        graph.ways.insert(way.id.0, r_way);
        build_info.ways.insert(way.id.0, g_way);


    }
}

fn new_cross(graph: &mut PostOsmGraph, src: u64, dst: u64, distance: f64, duration: f64, start_forward: bool, end_forward: bool) {

    if src == dst {
        return;
    }

    let duration = (duration * 1000.0) as i32;
    let a = ((src as u128) << 64) | dst as u128;



    if let Some(cd) = graph.cross_datas.get_mut(&a) {

        if cd.duration > duration {
            cd.distance = distance;
            cd.duration = duration;
            cd.start_forward = start_forward;
            cd.end_forward = end_forward;
            graph.cd_map.get_mut(&src).unwrap().iter_mut().find(|(k, _)| *k == dst).unwrap().1 = duration;
            graph.cd_map_reverse.get_mut(&dst).unwrap().iter_mut().find(|(k, _)| *k == src).unwrap().1 = duration;
        }


    } else {
        graph.cross_datas.insert(a,  CrossData { start_mid_point_id: src, end_mid_point_id: dst, distance, duration, start_forward, end_forward, center_mid_point_id: 0 } );

        graph.cd_map.entry(src).or_insert(vec![]).push((dst, duration));
        graph.cd_map_reverse.entry(dst).or_insert(vec![]).push((src, duration));

    }


}

//create midpoints
pub(crate) fn create_mid_points(graph: &mut PostOsmGraph, mut build_info: GraphBuildInfo) {


    for (_, way) in & mut build_info.ways {
        let mut multi_way_node_indexes: Vec<usize> = vec![];


        for i in 0..way.nodes.len() {
            let node_id = way.nodes[i];
            let way_ids = build_info.node_id_to_way_ids.get(&node_id).unwrap();
            if way_ids.len() > 1 {
                multi_way_node_indexes.push(i);
            }
        }


        if !multi_way_node_indexes.is_empty() {

            if multi_way_node_indexes.len() > 1 {

                for i in 0..multi_way_node_indexes.len() - 1 {
                    let start = multi_way_node_indexes[i];
                    let end = multi_way_node_indexes[i + 1];


                    way.after_mid_points[start] = end;
                    way.before_mid_points[end] = start;
                }
            }


            let first = multi_way_node_indexes[0];

            if first > 0 {
                way.before_mid_points[first] = 0;
                way.after_mid_points[0] = first;
            }

            let last = multi_way_node_indexes[multi_way_node_indexes.len() - 1];

            if last < way.nodes.len() - 1 {
                way.after_mid_points[last] = way.nodes.len() - 1;
                way.before_mid_points[way.nodes.len() - 1] = last;
            }

            let get_coord = |g: usize| -> [f64; 2] {
                let nn1 = build_info.nodes.get(way.nodes.get(g).unwrap()).unwrap();
                let nn2 = build_info.nodes.get(way.nodes.get(g + 1).unwrap()).unwrap();

                [((nn1.lon + nn2.lon) / 2.0), ((nn1.lat + nn2.lat) / 2.0)]
            };


            for (mp_end, mp_start) in way.before_mid_points.iter().enumerate() {

                if mp_start == &usize::MAX {
                    continue;
                }

                let s = mp_start + mp_end;

                let coord = if s % 2 == 0 {
                    let nn = build_info.nodes.get(way.nodes.get(s/2).unwrap()).unwrap();
                    [nn.lon, nn.lat]
                } else { get_coord(s/2) };

                let m_id = encode_to_mid_point_id(way.id, *mp_start, mp_end);
                graph.mid_point_id_to_coord.insert(m_id, coord);

                 graph.rtree.insert(Rect::new_point(coord), m_id);
            }
        }
    }


    for (_, way) in &build_info.ways {




        let result = build_info.way_id_to_result.get(&way.id).unwrap();


        for (end, start) in way.before_mid_points.iter().enumerate() {

            if start == &usize::MAX {
                continue;
            }



            let src_id = encode_to_mid_point_id(way.id, *start, end);
            let s = start + end;
            let middle  = s / 2;

            let mut before_distance = way.distances[middle] - way.distances[*start];
            let mut after_distance = way.distances[end] - way.distances[middle];

            if s % 2 == 1 {
                let h = (way.distances[middle + 1] - way.distances[middle])/2.0;
                before_distance += h;
                after_distance -= h;
            }


            let start_node_id = way.nodes[*start];
            let before_crossed_ways = build_info.node_id_to_way_ids.get(&start_node_id).unwrap();


            for other_before_way_id in before_crossed_ways {

                let other_way = build_info.ways.get(other_before_way_id).unwrap();


                let other_result = build_info.way_id_to_result.get(other_before_way_id).unwrap();
                let other_node_index = other_way.node_id_to_index.get(&start_node_id).unwrap();
                let l = other_way.nodes.len() -1;


                let other_before_mid_point = if other_node_index != &0 {
                    Some((other_way.before_mid_points[*other_node_index], other_node_index))
                } else if other_way.nodes[0] == other_way.nodes[l] {
                    Some((other_way.before_mid_points[l], &l))
                } else {
                    None
                };

                let other_after_mid_point = if other_node_index != &l {
                    Some((other_node_index, other_way.after_mid_points[*other_node_index]))
                } else if other_way.nodes[0] == other_way.nodes[l] {
                    Some((&0, other_way.after_mid_points[0]))
                } else {
                    None
                };


                if let Some((other_start, other_end)) = other_before_mid_point {
                    let other_way_before_id = encode_to_mid_point_id(*other_before_way_id, other_start, *other_end);

                    let s = other_start + other_end;
                    let middle  = s / 2;

                    let mut other_before_distance = other_way.distances[*other_end] - other_way.distances[middle];

                    if s % 2 == 1 {
                        other_before_distance -= (other_way.distances[middle+1] - other_way.distances[middle]) / 2.0;
                    }

                    if result.backward_mode == WayMode::Walk && other_result.backward_mode == WayMode::Walk {
                        let dur = before_distance / result.backward_speed + other_before_distance / other_result.backward_speed;
                        new_cross(graph, src_id, other_way_before_id, before_distance + other_before_distance, dur, false, false);
                    }
                }

                if let Some((other_start, other_end)) = other_after_mid_point {
                    let other_way_after_id = encode_to_mid_point_id(*other_before_way_id, *other_start, other_end);

                    let s = other_start + other_end;
                    let middle  = s / 2;

                    let mut other_after_distance = other_way.distances[middle] - other_way.distances[*other_start];

                    if s % 2 == 1 {
                        other_after_distance += (other_way.distances[middle+1] - other_way.distances[middle]) / 2.0;
                    }

                    if result.backward_mode == WayMode::Walk && other_result.forward_mode == WayMode::Walk {
                        let dur = before_distance / result.backward_speed + other_after_distance / other_result.forward_speed;
                        new_cross(graph, src_id, other_way_after_id, before_distance + other_after_distance, dur, false, true);
                    }

                }


            }

            let after_node_id = way.nodes[end];
            let after_crossed_ways = build_info.node_id_to_way_ids.get(&after_node_id).unwrap();

            for other_after_way_id in after_crossed_ways {

                let other_way = build_info.ways.get(other_after_way_id).unwrap();
                let other_result = build_info.way_id_to_result.get(other_after_way_id).unwrap();
                let other_node_index = other_way.node_id_to_index.get(&after_node_id).unwrap();
                let l = other_way.nodes.len() -1;


                let other_before_mid_point = if other_node_index != &0 {
                    Some((other_way.before_mid_points[*other_node_index], other_node_index))
                } else if other_way.nodes[0] == other_way.nodes[l] {
                    Some((other_way.before_mid_points[l], &l))
                } else {
                    None
                };

                let other_after_mid_point = if other_node_index != &l {
                    Some((other_node_index, other_way.after_mid_points[*other_node_index]))
                } else if other_way.nodes[0] == other_way.nodes[l] {
                    Some((&0, other_way.after_mid_points[0]))
                } else {
                    None
                };


                if let Some((other_start, other_end)) = other_before_mid_point {
                    let other_way_before_id = encode_to_mid_point_id(*other_after_way_id, other_start, *other_end);

                    let s = other_start + other_end;
                    let middle  = s / 2;

                    let mut other_before_distance = other_way.distances[*other_end] - other_way.distances[middle];

                    if s % 2 == 1 {
                        other_before_distance -= (other_way.distances[middle+1] - other_way.distances[middle]) / 2.0;
                    }

                    if result.forward_mode == WayMode::Walk && other_result.backward_mode == WayMode::Walk {
                        let dur = after_distance / result.forward_speed + other_before_distance / other_result.backward_speed;
                        new_cross(graph, src_id, other_way_before_id, after_distance + other_before_distance, dur, true, false);
                    }
                }

                if let Some((other_start, other_end)) = other_after_mid_point {
                    let other_way_after_id = encode_to_mid_point_id(*other_after_way_id, *other_start, other_end);

                    let s = other_start + other_end;
                    let middle  = s / 2;

                    let mut other_after_distance = other_way.distances[middle] - other_way.distances[*other_start];

                    if s % 2 == 1 {
                        other_after_distance += (other_way.distances[middle+1] - other_way.distances[middle]) / 2.0;
                    }

                    if result.forward_mode == WayMode::Walk && other_result.forward_mode == WayMode::Walk {
                        let dur = after_distance / result.forward_speed + other_after_distance / other_result.forward_speed;
                        new_cross(graph, src_id, other_way_after_id, after_distance + other_after_distance, dur, true, true);
                    }
                }
            }
        }
    }

    // return graph;
}


