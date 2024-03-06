use std::cmp::{max, min, Ordering, Reverse};
use std::sync::Arc;
use std::{thread, usize};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};
use ahash::{AHashMap as HashMap, AHashMap, AHashSet as HashSet, AHashSet};
use ntex::util::join_all;
use ntex::web::test::config;
use radix_heap::RadixHeapMap;
use crate::graph_builder::{CrossData, PostOsmGraph, WayRoutingInfo};
use priority_queue::PriorityQueue as ChPq;
use crate::encoding_utils::get_cross_data_key;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;



pub(crate) struct OverlayGraph {
    pub(crate) ranks: Vec<u64>,
    pub(crate) contractions: Vec<([usize; 3], i32)>,
}

pub(crate) struct SlowGraphCrossData {
    pub(crate) adg: usize,
    pub(crate) weight: i32,
    pub(crate) center_node: usize,
}


pub(crate) struct SlowGraph {
    pub(crate) mid_points_order: Vec<u64>,
    pub(crate) out_goings: Vec<Vec<SlowGraphCrossData>>,
    pub(crate) in_comings: Vec<Vec<SlowGraphCrossData>>,
    pub(crate) nodes_len: usize,

}

pub(crate) struct FastGraph {
    pub(crate) nodes_len: usize,
    pub(crate) ways: HashMap<i64, WayRoutingInfo>,
    pub(crate) mid_point_coords: Vec<[f64; 2]>,
    pub(crate) ranks: Vec<u64>,
    pub(crate) ocd: Vec<CrossData>,
    pub(crate) icd: Vec<CrossData>,
}


pub(crate) fn load_post_osm_to_slow_graph(pbf_path: &Path, graph : &mut SlowGraph) {
    let path = pbf_path.parent().unwrap().join("post_osm.bin");
    let mut file = File::open(path.as_path()).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    decode_from_byte_array_to_slow_graph(buf, graph)
}

pub(crate) fn decode_from_byte_array_to_slow_graph(buf: Vec<u8>, sg: & mut SlowGraph) {
    let mut index = 0;
    let ways_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
    index += 4;
    for _ in 0..ways_len {
        index += 8;
        let nodes_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
        index += 4;
        index += 16 * nodes_len as usize;
    }

    let mid_point_id_to_coord_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
    sg.nodes_len = mid_point_id_to_coord_len as usize;
    index += 4;
    let mut mid_pint_id_to_index = HashMap::with_capacity(mid_point_id_to_coord_len as usize);
    for idx in 0..mid_point_id_to_coord_len {
        let mid_point_id = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
        index += 24;
        mid_pint_id_to_index.insert(mid_point_id, idx as usize);
        sg.mid_points_order.push(mid_point_id);
        sg.out_goings.push(Vec::with_capacity(6));
        sg.in_comings.push(Vec::with_capacity(6));
    }

    let cross_dates_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
    let mut edges = HashMap::with_capacity(cross_dates_len as usize);
    index += 4;
    for _ in 0..cross_dates_len {
        let src = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
        index += 8;
        let dst = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
        index += 16;
        let duration = i32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
        index += 5;
        edges.insert(get_cross_data_key(src, dst), duration);
    }

    let cd_map_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());

    index += 4;
    for _ in 0..cd_map_len {
        let src = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
        let src_idx = mid_pint_id_to_index[&src];
        let mut vv = &mut sg.out_goings[src_idx];
        index += 8;
        let outgoing_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
        index += 4;
        for _ in 0..outgoing_len {
            let dst = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
            index += 12;
            let dst_idx = mid_pint_id_to_index[&dst];
            vv.push(SlowGraphCrossData {
                adg: dst_idx,
                weight: edges[&get_cross_data_key(src, dst)],
                center_node: usize::MAX
            });
        }
    }

    let cd_map_rev_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
    index += 4;

    for _ in 0..cd_map_rev_len {
        let dst = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
        let dst_idx = mid_pint_id_to_index[&dst];
        let vv = &mut sg.in_comings[dst_idx];
        index += 8;
        let incoming_len = u32::from_le_bytes(buf[index..index + 4].try_into().unwrap());
        index += 4;
        for _ in 0..incoming_len {
            let src = u64::from_le_bytes(buf[index..index + 8].try_into().unwrap());
            index += 12;
            let src_idx = mid_pint_id_to_index[&src];
            vv.push(SlowGraphCrossData {
                adg: src_idx,
                weight: edges[&get_cross_data_key(src, dst)],
                center_node: usize::MAX,
            });
        }
    }
}

pub(crate) fn run_contraction(sg: &mut SlowGraph, og: &mut OverlayGraph) {
    let mut counter = 0;
    let nodes_len = sg.mid_points_order.len();

    let mut levels = vec![0; nodes_len];

    let mut queue: ChPq<usize, i32> = ChPq::with_capacity(nodes_len);

    // let mut dijkstra = LocalDijkstra::new(nodes_len, 0 );
    let mut dijkstra = LocalDijkstra::new(nodes_len, 0 );

    let mut num_of_dijkstra = 10;

    let mut lds = Vec::with_capacity(num_of_dijkstra);

    for idx in 0..num_of_dijkstra {
        lds.push(LocalDijkstra::new(nodes_len, idx));
    }

    let nodes = (0..nodes_len).collect::<Vec<_>>();
    println!("start calc relevance for {} nodes", nodes_len);
    let pp = Instant::now();
    for mid_point in nodes {
        let priority = calc_relevance(sg, mid_point, 0, &mut lds);
        queue.push(mid_point, priority as i32);
        counter += 1;
        if counter % 100000 == 0 {
            println!("counter: {}", counter);
        }
    }


    println!("end calc relevance in {:?}", pp.elapsed());



    let mut sctt = Duration::default();
    let mut srtt = Duration::default();
    let mut whole_time = Instant::now();

    let total_durations = Instant::now();

    while let Some((node, _)) = queue.pop() {
        if og.ranks.len() % 10000 == 0 {
            println!("ctr: {}, cts: {}, sctt: {:?}, scrtt: {:?}, wholetime: {:?}, total dur: {:?}",
                     og.ranks.len(), og.contractions.len(), sctt, srtt, whole_time.elapsed(), total_durations.elapsed());
            sctt = Duration::default();
            srtt = Duration::default();
            whole_time = Instant::now();
        }
        og.ranks.push(sg.mid_points_order[node]);


        let mut neighbors = HashSet::new();


        for ocd in &sg.out_goings[node] {
            neighbors.insert(ocd.adg);
            if ocd.center_node != usize::MAX {
                og.contractions.push(([node, ocd.center_node, ocd.adg], ocd.weight));
            }

        }

        for icd in &sg.in_comings[node] {
            neighbors.insert(icd.adg);
            if icd.center_node != usize::MAX {
                og.contractions.push(([icd.adg, icd.center_node, node], icd.weight));
            }
        }

        let sct = Instant::now();
        contract_node(sg, node, &mut dijkstra);
        sctt += sct.elapsed();

        let srt = Instant::now();

        for neighbor in &neighbors {
            levels[*neighbor] = max(levels[*neighbor], levels[node] + 1);
            let priority =  calc_relevance(
                sg,
                *neighbor,
                levels[*neighbor],
                &mut lds
            );
            queue.change_priority(neighbor, priority as i32);
        }

        srtt += srt.elapsed();

    }
    println!("akhari, counter: {}, contractions: {},  sctt: {:?}, scrtt: {:?}, wholetime: {:?}, total duration: {:?}",
             og.ranks.len(), og.contractions.len(), sctt, srtt, whole_time.elapsed(), total_durations.elapsed());

}



fn cut_node(graph: &mut SlowGraph, node: usize) {
    for ocd in graph.out_goings[node].iter() {
        graph.in_comings.get_mut(ocd.adg).unwrap().retain(|x| x.adg != node);
    }
    graph.out_goings[node].clear();

    for icd in graph.in_comings[node].iter() {
        graph.out_goings.get_mut(icd.adg).unwrap().retain(|x| x.adg != node);
    }
    graph.in_comings[node].clear();
}

fn add_or_reduce_shortcut(graph: &mut SlowGraph, in_arc: usize, out_arc: usize, new_weight: i32, center_node: usize) {
    let mut found = false;
    for ocd in graph.out_goings[in_arc].iter_mut() {
        if ocd.adg == out_arc {
            if ocd.weight > new_weight {
                ocd.weight = new_weight;
                ocd.center_node = center_node;
                for icd in graph.in_comings[out_arc].iter_mut() {
                    if icd.adg == in_arc {
                        icd.weight = new_weight;
                        icd.center_node = center_node;
                    }
                }
            }


            found = true;
            break;
        }
    }
    if !found {
        graph.out_goings[in_arc].push(SlowGraphCrossData {
            adg: out_arc,
            weight: new_weight,
            center_node
        });
        graph.in_comings[out_arc].push(SlowGraphCrossData {
            adg: in_arc,
            weight: new_weight,
            center_node
        });
    }
}


fn contract_node(graph: &mut SlowGraph, node: usize, dijkstra: &mut LocalDijkstra) {

    if !graph.in_comings[node].is_empty() && !graph.out_goings[node].is_empty() {
        dijkstra.set_avoid_node(node);

        for i in 0..graph.in_comings[node].len() {
            let icd = &graph.in_comings[node][i];
            let in_arc = icd.adg;
            let in_dur = icd.weight;
            dijkstra.set_start_node(in_arc);
            for j in 0..graph.out_goings[node].len() {
                let ocd = &graph.out_goings[node][j];
                let out_arc = ocd.adg;
                let out_dur = ocd.weight;
                if in_arc == out_arc {
                    continue;
                }
                let new_weight = in_dur + out_dur;
                let weight = dijkstra.local_dijkstra_calc_weight(graph, out_arc, new_weight);
                if weight.is_none() || weight.unwrap() > new_weight {
                    add_or_reduce_shortcut(graph, in_arc, out_arc, new_weight, node);
                }
            }
        }

    }

    cut_node(graph, node);

}

fn calc_relevance(sg: &SlowGraph, node: usize, level: usize, lds: &mut Vec<LocalDijkstra>) -> f32 {
    let mut num_shortcuts = par_count_shortcuts(sg, node, lds);
    let num_edges = sg.out_goings[node].len() + sg.in_comings[node].len();
    let mut relevance = (0.1 * level as f32) + (num_shortcuts as f32 + 1.0) / (num_edges as f32 + 1.0);
    relevance * -1000.0
}

pub(crate) fn run_contraction_with_order(sg: &mut SlowGraph, og: &mut OverlayGraph, order: Vec<usize>) {
    let mut counter = 0;

    println!("order length: {}", order.len());

    let mut dijkstra = LocalDijkstra::new(order.len(), 0);


    for node in order {
        counter += 1;
        // og.ranks.push(node);

        if counter % 10000 == 0 {
            println!("counter: {}, {}", counter, og.contractions.len());
        }


        for ocd in sg.out_goings[node].iter() {
                if ocd.center_node != usize::MAX {
                    og.contractions.push(([node, ocd.center_node, ocd.adg], ocd.weight));
                }
        }

        for icd in sg.in_comings[node].iter() {
            if icd.center_node != usize::MAX {
                og.contractions.push(([icd.adg, icd.center_node, node], icd.weight));
            }

        }


        contract_node(sg, node, &mut dijkstra);
    }
}


fn count_shortcuts(sg: &SlowGraph, node: usize, dijkstra: &mut LocalDijkstra) -> usize {
    let ins_len = sg.in_comings[node].len();
    let outs_len = sg.out_goings[node].len();
    if ins_len == 0 || outs_len == 0 {
        return 0;
    }

    dijkstra.set_avoid_node(node);

    let mut ans = 0;

    for icd in &sg.in_comings[node] {
         dijkstra.set_start_node(icd.adg);
        for ocd in &sg.out_goings[node] {
            let new_weight  = icd.weight + ocd.weight;
            let weight = dijkstra.local_dijkstra_calc_weight(sg, ocd.adg, new_weight);
            if weight.is_none() || (weight.unwrap() > new_weight) {
                ans += 1;
            }
        }
    }

    ans
}


fn par_count_shortcuts(sg: &SlowGraph, node: usize, lds: &mut Vec<LocalDijkstra>) -> usize {
    let ins_len = sg.in_comings[node].len();
    let outs_len = sg.out_goings[node].len();
    let lds_len = lds.len();
    if ins_len == 0 || outs_len == 0 {
        return 0;
    }

    if ins_len < 7 {
        return count_shortcuts(sg, node, &mut lds[0]);
    }


    let s_len = min(ins_len / 5, lds_len);

    lds[..s_len].par_iter_mut().map(|dijkstra| {
        dijkstra.set_avoid_node(node);
        let mut ans = 0;
        let mut i = dijkstra.idx;
        while i < ins_len {
            let inn = &sg.in_comings[node][i];
            dijkstra.set_start_node(inn.adg);
            for ocd in &sg.out_goings[node] {
                let new_weight  = inn.weight + ocd.weight;
                let weight = dijkstra.local_dijkstra_calc_weight(sg, ocd.adg, new_weight);
                if weight.is_none() || (weight.unwrap() > new_weight) {
                    ans += 1;
                }
            }
            i += s_len;
        };
        ans
    }).sum::<usize>()



}


#[derive(Eq, Copy, Clone, Debug)]
pub struct HeapItem {
    pub weight: i32,
    pub node_id: usize,
}

impl HeapItem {
    pub fn new(weight: i32, node_id: usize) -> HeapItem {
        HeapItem { weight, node_id }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &HeapItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &HeapItem) -> Ordering {
        self.weight.cmp(&other.weight).reverse()
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &HeapItem) -> bool {
        self.weight == other.weight
    }
}



pub struct LocalDijkstra {
    queue: BinaryHeap<HeapItem>,
    durs: Vec<i32>,
    visited: Vec<bool>,
    valids: Vec<usize>,
    nodes_len: usize,
    start_node: usize,
    avoid_node: usize,
    duration_limit: i32,
    valid_value: usize,
    idx: usize
}

impl LocalDijkstra {
    pub fn new(nodes_len: usize, idx: usize) -> Self {
        let queue = BinaryHeap::with_capacity(nodes_len);
        let durs = vec![0; nodes_len];
        let visited = vec![false; nodes_len];
        let valids = vec![0; nodes_len];
        let valid_value = 1;

        Self {
            queue,
            durs,
            visited,
            valids,
            valid_value,
            nodes_len,
            start_node: usize::MAX,
            avoid_node: usize::MAX,
            duration_limit: i32::MAX,
            idx
        }
    }


    pub fn invalidate(&mut self) {

        if self.valid_value +1 == usize::MAX {
            self.valid_value = 1;
            self.valids = vec![0; self.nodes_len];
        } else {
            self.valid_value += 1;
        }
    }

    pub fn set_avoid_node(&mut self, avoid_node: usize) {
        self.avoid_node = avoid_node;
        self.start_node = usize::MAX;
    }

    pub fn set_start_node(&mut self, start_node: usize) {

        if self.start_node != start_node {
            self.queue.clear();
            self.invalidate();
            self.start_node = start_node;
            self.queue.push(HeapItem::new(0, start_node));
            self.durs[start_node] = 0;
            self.visited[start_node] = false;
            self.valids[start_node] = self.valid_value;
        }
    }



    pub fn is_valid(&self, index: usize) -> bool {
        self.valids[index] == self.valid_value
    }


    pub fn is_visited(&self, index: usize) -> bool {
        self.is_valid(index) && self.visited[index]
    }



    fn local_dijkstra_calc_weight(&mut self, graph: &SlowGraph, target: usize, duration_limit: i32) -> Option<i32> {

        self.duration_limit = duration_limit;

        if self.is_visited(target) {
            return Some(self.durs[target]);
        }

        while let Some(hi) = self.queue.pop() {
            let u = hi.node_id;

            let u_duration = hi.weight;

            if self.is_visited(u) {
                continue;
            }

            self.visited[u] = true;

            for ocd in &graph.out_goings[u] {
                let v = ocd.adg;
                let v_dur = ocd.weight;
                if self.is_visited(v) || v == self.avoid_node {
                    continue;
                }

                let alt = u_duration + v_dur;


                if !self.is_valid(v) || alt < self.durs[v] {
                    self.queue.push(HeapItem::new(alt, v));
                    self.durs[v] = alt;
                    self.valids[v] = self.valid_value;
                    self.visited[v] = false;
                }
            }

            if u_duration >= self.duration_limit || u == target {
                break;
            }

        }
        if self.is_valid(target) {
            Some(self.durs[target])
        } else {
            None
        }
    }
}
