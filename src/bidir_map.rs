use std::{
    collections::{hash_map::Keys, HashMap}, hash::Hash,
};
macro_rules! bidir_map {
	(@single $($x:tt)*) => (());
	(@count $($rest:expr),*) => (<[()]>::len(&[$(bidir_map!(@single $rest)),*]));

	// Ideally the separator would be <=> instead of => but it's parsed as <= > and therefore illegal
	($($key:expr => $value:expr,)+) => { bidir_map!($($key => $value),+) };
	($($key:expr => $value:expr),*) => {{
		let cap = bidir_map!(@count $($key),*);
		let mut map = ::bidir_map::BidirMap::with_capacity(cap);
		$(map.insert($key, $value);)*
		map
	}};
}

pub struct BidirMap<Kv1: PartialEq + Eq + Hash + Clone, Kv2: PartialEq + Eq + Hash + Clone> {
    first_second: HashMap<Kv1, Kv2>,
    second_first: HashMap<Kv2, Kv1>,
}

impl<Kv1: PartialEq + Eq + Hash + Clone, Kv2: PartialEq + Eq + Hash + Clone> BidirMap<Kv1, Kv2> {
    /*
    pub fn new() -> Self {
        BidirMap {
            first_second: HashMap::new(),
            second_first: HashMap::new(),
        }
    }*/

    pub fn with_capacity(capacity: usize) -> Self {
        BidirMap {
            first_second: HashMap::with_capacity(capacity),
            second_first: HashMap::with_capacity(capacity),
        }
    }

    /*
    pub fn clear(&mut self) {
        self.first_second.clear();
        self.second_first.clear();
    }
*/
    pub fn insert(&mut self, kv1: Kv1, kv2: Kv2) {
        //  self.remove_by_first(&kv1);
        //  self.remove_by_second(&kv2);
        self.second_first.insert(kv2.clone(), kv1.clone());
        self.first_second.insert(kv1, kv2);
    }
    /*
    pub fn iter_first_second(&self) -> Keys<Kv1, Kv2> {
        self.first_second.keys()
    }
*/
    pub fn iter_second_first(&self) -> Keys<Kv2, Kv1> {
        self.second_first.keys()
    }
    /*
    pub fn len(&self) -> usize {
        self.first_second.len()
    }

    pub fn is_empty(&self) -> bool {
        self.first_second.is_empty()
    }
*/
    pub fn get_by_first(&self, key: &Kv1) -> Option<&Kv2> {
        self.first_second.get(key)
    }

    pub fn get_by_second(&self, key: &Kv2) -> Option<&Kv1> {
        self.second_first.get(key)
    }
}
