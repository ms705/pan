use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use distributary::{ActivationResult, Blender, DataType, Getter, Mutator, Recipe, MutatorError};
use distributary::web;

type Datas = Vec<Vec<DataType>>;

pub struct Backend {
    getters: HashMap<String, Getter>,
    mutators: HashMap<String, Mutator>,
    recipe: Option<Recipe>,
    queries: HashMap<String, (String, usize)>,
    pub soup: Arc<Mutex<Blender>>,
}

impl Backend {
    pub fn new(soup: Blender, recipe: Recipe) -> Backend {
        let soup = Arc::new(Mutex::new(soup));
        let soup2 = soup.clone();
        thread::spawn(|| web::run(soup2));

        Backend {
            getters: HashMap::default(),
            mutators: HashMap::default(),
            recipe: Some(recipe),
            queries: HashMap::default(),
            soup: soup,
        }
    }

    pub fn migrate(&mut self, line: &str) -> Result<ActivationResult, String> {
        let prev_recipe = self.recipe.clone();
        // try to add query to recipe
        match self.recipe.take().unwrap().extend(line) {
            Ok(mut new_recipe) => {
                let mut soup = self.soup.lock().unwrap();
                let mut mig = soup.start_migration();
                match new_recipe.activate(&mut mig, false) {
                    Ok(act_res) => {
                        mig.commit();
                        self.recipe = Some(new_recipe);
                        Ok(act_res)
                    }
                    Err(e) => {
                        self.recipe = prev_recipe;
                        Err(e)
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn put(&mut self, kind: &str, data: &[DataType]) -> Result<(), String> {
        let mtr =
            self.mutators
                .entry(String::from(kind))
                .or_insert(self.soup
                               .lock()
                               .unwrap()
                               .get_mutator(self.recipe.as_ref().unwrap().node_addr_for(kind)?));

        mtr.put(data)
            .map_err(|e| match e {
                         MutatorError::WrongColumnCount(expected, got) => {
                             format!("Wrong number of columns specified: expected {}, got {}",
                                     expected,
                                     got)
                         }
                         MutatorError::TransactionFailed => unreachable!(),
                     })
    }

    pub fn add_query(&mut self, name: &str, kind: &str, num_params: usize) {
        self.queries.insert(name.into(), (kind.into(), num_params));
    }

    pub fn execute_query(&mut self, name: &str, params: &[DataType]) -> Result<Datas, String> {
        if params.len() != 1 {
            return Err(format!("Only single parameter queries are currently supported"));
        }

        let (ref kind, nparams) = *match self.queries.get(name) {
            Some(k) => k,
            None => return Err(format!("Unrecognized query: \"{}\"", name)),
        };

        if nparams != params.len() {
            return Err(format!("Wrong number of values: expected {}, got {}",
                               nparams,
                               params.len()));
        }

        let getter = self.getters
            .entry(kind.clone())
            .or_insert(self.soup
                           .lock()
                           .unwrap()
                           .get_getter(self.recipe.as_ref().unwrap().node_addr_for(kind)?)
                           .unwrap());

        match getter.lookup(&params[0], true) {
            Ok(records) => Ok(records),
            Err(_) => Err(format!("GET for {} failed!", kind)),
        }
    }

    // pub fn get<I>(&mut self, kind: &str, key: I) -> Result<Datas, String>
    //     where I: Into<DataType>
    // {
    //     let get_fn = self.getters
    //         .entry(String::from(kind))
    //         .or_insert(self.soup
    //                        .lock()
    //                        .unwrap()
    //                        .get_getter(self.recipe.as_ref().unwrap().node_addr_for(kind)?)
    //                        .unwrap());

    //     match get_fn(&key.into(), true) {
    //         Ok(records) => Ok(records),
    //         Err(_) => Err(format!("GET for {} failed!", kind)),
    //     }
    // }

    pub fn query_exists(&self, name: &str) -> bool {
        self.queries.contains_key(name)
    }
}
