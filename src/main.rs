#![allow(unused)]

mod distance_utils;
mod ellipse;
mod graph_builder;
mod osm_handlers;
mod encoding_utils;
mod line_encoding;
mod gtfs;
mod contraction_hierarchies;



use std::collections::{BTreeSet};
use std::fs::File;
use std::io::Write;

use std::sync::Arc;
use std::time::Instant;
use smartstring::alias::String;
use ntex::web;
use serde::{Deserialize, Serialize};
use ntex::web::HttpResponse;
use ellipse::Ellipse;
use ahash::{AHashMap as HashMap, AHashSet as HashSet, AHashSet};
use radix_heap::{RadixHeapMap};
use rtree_rs::{Rect};
use graph_builder::{PostOsmGraph, read_pbf, create_mid_points, GraphBuildInfo};
use osm_handlers::OsmProfile;
use encoding_utils::decode_mid_point_id;
use clap::Parser;
use crate::graph_builder::{CrossData, load_hc, save_graph};
use crate::contraction_hierarchies::{load_post_osm_to_slow_graph, OverlayGraph, run_contraction, run_contraction_with_order, SlowGraph, SlowGraphCrossData};
use crate::encoding_utils::get_cross_data_key;


// time curl 'http://localhost:6060/routes?src_lat=37.461572577590786&src_long=-122.23255112102916&dst_lat=37.322502012298074&dst_long=-121.84757519048424'


const PRECISION_FLOAT: f64 = 10_000_000.0;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    // #[arg(short, long)]
    // name: String,

    #[arg(long)]
    path: std::path::PathBuf,

    /// auto inc minor
    #[clap(long)]
    osm_process: bool,

    #[clap(long)]
    contract: bool,

    /// auto inc minor
    #[clap(long)]
    serve: bool,

}

#[derive(Deserialize)]
struct RouteInfo {
    src_lat: f64,
    src_long: f64,
    dst_lat: f64,
    dst_long: f64,
}


fn snap_to_way(graph: &PostOsmGraph, lat: f64, lon: f64) -> Result<u64, ()> {
    let target = Rect::new_point([lon, lat]);

    return  graph.rtree.nearby(|rect, _| rect.box_dist(&target)).next().map(|item| *item.data).ok_or(());
}

struct MidPointData {
    id: u64,
    prev_id: u64,
    prev_index: usize,
    duration: i32,
    distance_from_destination_km: f64,
    score: i32,
}


impl MidPointData {
    fn new(id: u64, prev_id: u64, prev_index: usize, duration: i32, distance_from_destination_km: f64) -> MidPointData {
        let heuristic = (distance_from_destination_km * 195_000.0) as i32;
        MidPointData {
            id,
            prev_id,
            prev_index,
            duration,
            distance_from_destination_km,
            score: duration + heuristic,
        }
    }
}



//
// async fn bi_dijkstra(graph: &PostOsmGraph, og: &OverlayGraph, src_id: u64, dst_id: u64, ellipse: &Ellipse) -> Option<(Vec<(u64, usize)>, i32)> {
//     // let mut queue: DaryHeap<i128, 4> = DaryHeap::new();
//     let mut fwd_queue: RadixHeapMap<i32, u64> = RadixHeapMap::new();
//     let mut bwd_queue: RadixHeapMap<i32, u64> = RadixHeapMap::new();
//
//     let mut fwd_infos: HashMap<u64, MidPointData> = HashMap::new();
//     let mut bwd_infos: HashMap<u64, MidPointData> = HashMap::new();
//     // let mut visited: HashMap<u64, MidPointData> = HashMap::new();
//     let mut fwd_visited: HashSet<u64> = HashSet::new();
//     let mut bwd_visited: HashSet<u64> = HashSet::new();
//
//     let mut meeting_point: Option<u64> = None;
//     let mut meeting_point_score: i32 = i32::MAX;
//
//
//     fwd_infos.insert(src_id, MidPointData::new(src_id, 0, 0, 0, 0.0));
//     fwd_queue.push(0, src_id);
//
//     bwd_infos.insert(dst_id, MidPointData::new(dst_id, 0, 0, 0, 0.0));
//     bwd_queue.push(0, dst_id);
//
//
//     while !fwd_queue.is_empty() || !bwd_queue.is_empty() {
//
//
//         while let Some((_, u_id)) = fwd_queue.pop() {
//             if fwd_visited.contains(&u_id) {
//                 continue;
//             }
//
//             let u_duration = fwd_infos.get(&u_id).unwrap().duration;
//             let u_rank = graph.ranks.get(&u_id).unwrap();
//
//             fwd_visited.insert(u_id);
//
//             if bwd_visited.contains(&u_id) {
//                 let total_duration = fwd_infos.get(&u_id).unwrap().duration + bwd_infos.get(&u_id).unwrap().duration;
//                 if total_duration < meeting_point_score {
//                     meeting_point = Some(u_id);
//                     meeting_point_score = total_duration;
//                 }
//             }
//
//
//             if let Some(m) = og.out_goings.get(&u_id) {
//                 for (idx, (v, v_dur)) in m.iter().enumerate() {
//                     if fwd_visited.contains(v) {
//                         continue;
//                     }
//
//                     let v_rank = graph.ranks.get(v).unwrap();
//
//                     if v_rank < u_rank {
//                         continue;
//                     }
//
//
//                     let alt = u_duration + v_dur;
//
//                     let v_cur_info = fwd_infos.get(v);
//
//                     if v_cur_info.is_none() || alt < v_cur_info.unwrap().duration {
//                         let m = MidPointData::new(*v, u_id, idx, alt, 0.0);
//                         fwd_queue.push(-m.duration, m.id);
//                         fwd_infos.insert(*v, m);
//                     }
//                 }
//             }
//             break;
//         }
//
//         while let Some((_, u_id)) = bwd_queue.pop() {
//             if bwd_visited.contains(&u_id) {
//                 continue;
//             }
//
//             let u_rank = graph.ranks.get(&u_id).unwrap();
//
//             let u_duration = bwd_infos.get(&u_id).unwrap().duration;
//
//             bwd_visited.insert(u_id);
//
//             if fwd_visited.contains(&u_id) {
//                 let total_duration = fwd_infos.get(&u_id).unwrap().duration + bwd_infos.get(&u_id).unwrap().duration;
//                 if total_duration < meeting_point_score {
//                     meeting_point = Some(u_id);
//                     meeting_point_score = total_duration;
//                 }
//             }
//
//
//             if let Some(m) = og.in_goings.get(&u_id) {
//                 for (idx, (v, v_dur)) in m.iter().enumerate() {
//                     if bwd_visited.contains(v) {
//                         continue;
//                     }
//
//                     let v_rank = graph.ranks.get(v).unwrap();
//
//                     if v_rank < u_rank {
//                         continue;
//                     }
//
//
//                     let alt = u_duration + v_dur;
//
//                     let v_cur_info = bwd_infos.get(v);
//
//                     if v_cur_info.is_none() || alt < v_cur_info.unwrap().duration {
//                         let m = MidPointData::new(*v, u_id, idx, alt, 0.0);
//                         bwd_queue.push(-m.duration, m.id);
//                         bwd_infos.insert(*v, m);
//                     }
//                 }
//             }
//             break
//         }
//     }
//
//     if meeting_point.is_some() {
//         let mut path: Vec<(u64, usize)> = vec![];
//         let duration = fwd_infos.get(&meeting_point.unwrap()).unwrap().duration + bwd_infos.get(&meeting_point.unwrap()).unwrap().duration;
//         // let u = infos.get(&u_id).unwrap();
//         // let mut cur = u;
//         // while cur.prev_id != 0 {
//         //     path.push((cur.id, cur.prev_index));
//         //     cur = infos.get(&cur.prev_id).unwrap();
//         // }
//         // path.push((cur.id, 0));
//         // path.reverse();
//         return Some((path, duration));
//     }
//     println!("no path found, {}, {}", fwd_infos.len(), bwd_infos.len());
//     //
//     // for (k, v) in fwd_infos.iter() {
//     //     println!("{}: {:?}", k, v.duration);
//     // }
//     // println!("fdfdf");
//     // for (k, v) in bwd_infos.iter() {
//     //     println!("{}: {:?}", k, v.duration);
//     // }
//     None
// }
//


// async fn dijkstra(graph: &PostOsmGraph, og: &OverlayGraph, src_id: u64, dst_id: u64, ellipse: &Ellipse) -> Option<(Vec<(u64, usize)>, i32)> {
//     // let mut queue: DaryHeap<i128, 4> = DaryHeap::new();
//     let mut queue: RadixHeapMap<i32, u64> = RadixHeapMap::new();
//     let mut infos: HashMap<u64, MidPointData> = HashMap::new();
//     // let mut visited: HashMap<u64, MidPointData> = HashMap::new();
//     let mut visited: HashSet<u64> = HashSet::new();
//
//
//     let src_distance_from_destination_km = ellipse.get_distance_from_destination(graph.mid_point_id_to_coord.get(&src_id).unwrap());
//     infos.insert(src_id, MidPointData::new(src_id, 0, 0, 0, src_distance_from_destination_km));
//     queue.push(0, src_id);
//
//     while let Some((_, u_id)) = queue.pop() {
//         if u_id == dst_id {
//             let mut path: Vec<(u64, usize)> = vec![];
//             let u = infos.get(&u_id).unwrap();
//             let mut cur = u;
//             while cur.prev_id != 0 {
//                 path.push((cur.id, cur.prev_index));
//                 cur = infos.get(&cur.prev_id).unwrap();
//             }
//             path.push((cur.id, 0));
//             path.reverse();
//             return Some((path, u.duration));
//         }
//
//
//         if visited.contains(&u_id) {
//             continue;
//         }
//
//         let u_duration = infos.get(&u_id).unwrap().duration;
//
//         visited.insert(u_id);
//
//         if let Some(m) = og.out_goings.get(&u_id) {
//             for (idx, (v, v_dur)) in m.iter().enumerate() {
//                 if visited.contains(v) {
//                     continue;
//                 }
//                 // let (contains, _, distance_from_destination_km) = ellipse.contains(graph.mid_point_id_to_coord.get(v).unwrap());
//                 // if !contains {
//                 //     contains;
//                 // }
//
//                 let distance_from_destination_km = ellipse.get_distance_from_destination(graph.mid_point_id_to_coord.get(v).unwrap());
//
//                 let alt = u_duration + v_dur;
//
//                 let v_cur_info = infos.get(v);
//
//                 if v_cur_info.is_none() || alt < v_cur_info.unwrap().duration {
//                     let m = MidPointData::new(*v, u_id, idx, alt, distance_from_destination_km);
//                     queue.push(-m.score, m.id);
//                     infos.insert(*v, m);
//                 }
//             }
//         }
//     }
//
//
//     println!("no path found, {}", infos.len());
//     None
// }


// async fn islands(graph: &PostOsmGraph) {
//     // let mut queue: DaryHeap<i128, 4> = DaryHeap::new();
//     // let mut visited: HashMap<u64, MidPointData> = HashMap::new();
//     let mut groups: Vec<HashSet<u64>> = vec![];
//
//     let all1 = graph.cd_map.keys().cloned().collect::<BTreeSet<u64>>();
//     let all2 = graph.cd_map_reverse.keys().cloned().collect::<BTreeSet<u64>>();
//
//     let mut all = all1.union(&all2).cloned().collect::<BTreeSet<u64>>();
//
//
//     while let Some(s_id) = all.pop_first() {
//
//
//         let mut group: HashSet<u64> = HashSet::new();
//         let mut queue: Vec<u64> = vec![];
//
//         queue.push(s_id);
//         group.insert(s_id);
//
//         while let Some(u_id) = queue.pop() {
//
//             if let Some(m1) = graph.cd_map.get(&u_id) {
//                 for (v,_) in m1 {
//                     if group.contains(v) {
//                         continue;
//                     }
//                     queue.push(*v);
//                     group.insert(*v);
//                     all.remove(v);
//                 }
//             }
//
//             if let Some(m2) = graph.cd_map_reverse.get(&u_id) {
//                 for (v, _) in m2 {
//                     if group.contains(v) {
//                         continue;
//                     }
//                     queue.push(*v);
//                     group.insert(*v);
//                     all.remove(v);
//                 }
//             }
//
//         }
//         // println!("group size: {}", group.len());
//         // println!("all size: {}", all.len());
//
//         groups.push(group);
//
//     }
//
//     println!("groups: {}", groups.len());
//
//     let gll30 = groups.iter().filter(|g| g.len() < 5).collect::<Vec<&HashSet<u64>>>();
//
//     println!("groups with size < 3: {}", gll30.len());
//
//     // for g in groups {
//     //     println!("group size: {}", g.len());
//     // }
//
// }



// async fn get_coordinates(graph: &PostOsmGraph, path: &[(u64, usize)]) -> Vec<[f64; 2]> {
//     let mut coordinates: Vec<[f64; 2]> = vec![];
//
//     let mut start_id = path[0].0;
//     let (mut start_way_id, mut start_way_first_index, mut start_way_second_index) = decode_mid_point_id(start_id);
//     let mut start_way = graph.ways.get(&start_way_id).unwrap();
//
//     for (end_id, idx) in path[1..].iter() {
//         let (end_way_id, end_way_first_index, end_way_second_index) = decode_mid_point_id(*end_id);
//         let cd_key = get_cross_data_key(start_id, *end_id);
//         let cd = graph.cross_datas.get(&cd_key).unwrap();
//
//
//         let s1 = start_way_first_index + start_way_second_index;
//
//
//         let m1 = graph.mid_point_id_to_coord.get(&start_id).unwrap();
//          coordinates.push([m1[0].to_degrees(), m1[1].to_degrees()]);
//
//         if cd.start_forward {
//             for j in (s1/2)+1..=start_way_second_index {
//                 coordinates.push(start_way.nodes_coord[j]);
//             }
//         } else {
//             for j in (start_way_first_index..(s1+1)/2).rev() {
//                 coordinates.push(start_way.nodes_coord[j]);
//             }
//
//         }
//
//
//         let end_way = graph.ways.get(&end_way_id).unwrap();
//
//         let s2 = end_way_first_index + end_way_second_index;
//         let m2 = graph.mid_point_id_to_coord.get(end_id).unwrap();
//
//         if cd.end_forward {
//             for j in end_way_first_index..((s2+1)/2) {
//                 coordinates.push(end_way.nodes_coord[j]);
//             }
//
//         } else {
//             for j in ((s2/2)+1..=end_way_second_index).rev() {
//                 coordinates.push(end_way.nodes_coord[j]);
//             }
//         }
//          coordinates.push([m2[0].to_degrees(), m2[1].to_degrees()]);
//
//
//
//         start_id = *end_id;
//         start_way_id = end_way_id;
//         start_way = end_way;
//         start_way_first_index = end_way_first_index;
//         start_way_second_index = end_way_second_index;
//     }
//
//
//     coordinates
// }

#[derive(Debug, Serialize, Deserialize)]
struct RoutingResponse {
    path: Vec<[f64; 2]>,
    eta: i32,
}


#[web::get("/islands")] // <- define path parameters
async fn get_islands(g: web::types::State<Arc<PostOsmGraph>>) -> HttpResponse {

    // islands(&g).await;

    HttpResponse::Ok().finish()

}



#[web::get("/routes")] // <- define path parameters
async fn get_routes(info: web::types::Query<RouteInfo>, g: web::types::State<Arc<PostOsmGraph>>, og: web::types::State<Arc<OverlayGraph>>) -> HttpResponse {
    let src_lat = info.src_lat.to_radians();
    let src_lon = info.src_long.to_radians();
    let dst_lat = info.dst_lat.to_radians();
    let dst_lon = info.dst_long.to_radians();

    let src_id = snap_to_way(&g, src_lat, src_lon).unwrap();
    let dst_id = snap_to_way(&g, dst_lat, dst_lon).unwrap();

    let ellipse = Ellipse::new(src_lat, src_lon, dst_lat, dst_lon);

    // println!("src_id: {}, dst_id: {}", src_id, dst_id);
    let before = Instant::now();
    // let (path, duration) = dijkstra(&g, src_id, dst_id, &ellipse).await.unwrap();
    // let (path, duration) = bi_dijkstra(&g, &og, src_id, dst_id, &ellipse).await.unwrap();
    let duration = 0;
    println!("Elapsed time: {:.2?}, {}", before.elapsed(), duration);

    let coordinates = vec![];
    // let coordinates = get_coordinates(&g, &path).await;

    // let coordinates = vec![[src_lat.to_degrees(), src_lon.to_degrees()], [dst_lat.to_degrees(), dst_lon.to_degrees()]];
    let response = RoutingResponse {
        path: coordinates,
        eta: duration as i32,
    };

    HttpResponse::Ok().json(&response)
}


async fn print_graph(graph :&SlowGraph) {
    let num_to_letter = vec!['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'];


    println!("asli");

    for (k, v) in graph.out_goings.iter().enumerate() {
        println!("{}", num_to_letter[k]);
        for ocd in v.iter() {
            println!("{} -> {}: ({:?})", k,  num_to_letter[ocd.adg], ocd.weight);
        }
    }
    println!("reverse");
    for (k ,v) in graph.in_comings.iter().enumerate() {
        println!("{}",  num_to_letter[k]);
        for icd in v.iter() {
            println!("{} -> {}: ({:?})", k,  num_to_letter[icd.adg], icd.weight);
        }
    }


}

async fn print_graph_og(graph :& OverlayGraph) {
    let num_to_letter = vec!['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'];
    for (path, dur) in graph.contractions.iter() {

        if path[1] != usize::MAX {
            println!("{} -> {} -> {}: {}", num_to_letter[path[0]],
                     num_to_letter[path[1]], num_to_letter[path[2]], dur);
        }
    }

    println!("asli");


}


async fn test_contraction() {
    let num_to_letter = vec!['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'];

    let data = vec![
        (0, 1, 3),
        (0, 2, 5),
        (0, 10, 3),

        (1, 2, 3),
        (1, 3, 5),

        (2, 3, 2),
        (2, 9, 2),

        (3, 4, 7),
        (3, 9, 4),

        (4, 5, 6),
        (4, 9, 3),

        (5, 6, 4),
        (5, 7, 2),

        (6, 7, 3),
        (6, 8, 5),

        (7, 8, 3),
        (7, 9, 2),

        (8, 9, 4),
        (8, 10, 6),

        (9, 10, 3),
    ];
    let order = vec![1, 4, 8, 10, 3, 6, 2, 9, 7, 5, 0]; // tut
    // let order = vec![8, 3, 5, 6, 4, 1, 7, 2, 9, 0, 10];
    let ellipse = Ellipse::new(0.0, 0.0, 0.0, 0.0);
    let nodes_len = order.len();

    let mut sg = SlowGraph {
        mid_points_order: (0..nodes_len).map(|x| x as u64).collect(),
        out_goings: vec![],
        in_comings: vec![],
        nodes_len,
    };

    for _ in 0..nodes_len {
        sg.out_goings.push(vec![]);
        sg.in_comings.push(vec![]);
    }

    for (src, dst, weight) in data {
        sg.out_goings[src].push(SlowGraphCrossData {
            adg: dst,
            weight,
            center_node: usize::MAX,
        });
        sg.in_comings[dst].push(SlowGraphCrossData {
            adg: src,
            weight,
            center_node: usize::MAX,
        });

        sg.out_goings[dst].push(SlowGraphCrossData {
            adg: src,
            weight,
            center_node: usize::MAX,
        });
        sg.in_comings[src].push(SlowGraphCrossData {
            adg: dst,
            weight,
            center_node: usize::MAX,
        });
    }

    print_graph(&sg).await;

    // println!("test fast path");
    // test_fast_graph(&sg);
    // println!("test fast path end");


    let mut og = OverlayGraph {
        ranks: vec![],
        contractions: vec![],
    };
    // print_graph_og(&og).await;
    run_contraction_with_order(&mut sg, &mut og, order.clone());
    //
    // // order the keys based on values
    //
    println!("custom order: {:?}", order.iter().map(|x| num_to_letter[*x as usize]).collect::<Vec<_>>());
    println!("order: {:?}", og.ranks);
    println!("order letters: {:?}", og.ranks.iter().map(|x| num_to_letter[*x as usize]).collect::<Vec<_>>());
    //
    //
    println!("After contraction, size: {}", og.contractions.len());
     print_graph_og(&og).await;

    //
    // let (_, dur) = bi_dijkstra(&graph, &og,  2, 7, &ellipse).await.unwrap();
    // println!("Duration: {}", dur);

}





#[ntex::main]
async fn main() -> std::io::Result<()> {
    // test_contraction().await;

    let args = Args::parse();

    // let args = Args {
    //     path: String::from("/Users/xhsvn/stanford/osm/stanford.osm.pbf").parse().unwrap(),
    //     osm_process: false,
    //     serve: false,
    //     contract: true,
    // };
    // return Ok(());

    // let pbf_path = String::from("/Users/xhsvn/stanford/osm/stanford.osm.pbf");

    if args.osm_process {
        let mut graph = PostOsmGraph::new();
        let profile = OsmProfile {
            access_tag_hierarchy: vec![String::from("foot"), String::from("access")],
            access_tag_black: HashSet::from([String::from("no"), String::from("private"),
                String::from("delivery"), String::from("agricultural"), String::from("forestry")]),
            speed: 5.0,
            surface_speed_factor: HashMap::from([(String::from("fine_gravel"), 0.8), (String::from("gravel"), 0.8),
                (String::from("pebblestone"), 0.8), (String::from("mud"), 0.5),
                (String::from("sand"), 0.5)]),
        };
        let mut graph_build_info = GraphBuildInfo {
            way_id_to_result: Default::default(),
            node_id_to_way_ids: Default::default(),
            nodes: HashMap::new(),
            ways: HashMap::new(),
        };
        read_pbf(&args.path, &mut graph, &mut graph_build_info, &profile);

        create_mid_points(&mut graph, graph_build_info);


        println!("{} ways", graph.ways.len());
        println!("{} mid points", graph.mid_point_id_to_coord.len());
        println!("{} cross data", graph.cross_datas.len());
        println!("{} cd map", graph.cd_map.len());
        println!("{} cd map reverse", graph.cd_map_reverse.len());
        // println!("{} result", graph.way_id_to_result.len());

        save_graph(&args.path, &graph);

    }


    if args.contract {

        let mut sg = SlowGraph {
            mid_points_order: vec![],
            out_goings: Default::default(),
            in_comings: Default::default(),
            nodes_len: 0,
        };
        load_post_osm_to_slow_graph(&args.path, &mut sg);
        println!("finish loading, now checking the size");

        let mut cn = 0;
        for i in 0..sg.out_goings.len() {
            cn += sg.out_goings[i].len();
        }
        println!("{} nodes, {} edges", sg.out_goings.len(), cn);

        let mut cn = 0;
        for i in 0..sg.in_comings.len() {
            cn += sg.in_comings[i].len();
        }
        println!("{} nodes, {} edges", sg.in_comings.len(), cn);



        let nodes_len = sg.mid_points_order.len();
        let mut og = OverlayGraph {
            ranks: Vec::with_capacity(nodes_len),
            contractions: Vec::with_capacity(nodes_len),
        };
        let before = Instant::now();

        run_contraction(&mut sg, &mut og);
        // test_fast_graph(&sg);
        println!("finish contraction in {:?} and {} new contractions", before.elapsed(), og.contractions.len());
    }





    // test_fast_graph(&graph).await;



//
//     let g = Arc::new(graph);
//     let og = Arc::new(og);

    // Ok(())

    if args.serve {
        web::HttpServer::new(move || web::App::new()
            // .state(g.clone())
            // .state(og.clone())
            .service(get_routes)
            .service(get_islands)
        )
            .bind(("127.0.0.1", 6060))?
            .run()
            .await
    } else {
        Ok(())
    }

}
