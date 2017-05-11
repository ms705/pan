use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use distributary::{ActivationResult, Blender, DataType, Mutator, Recipe};
use distributary::web;

type Datas = Vec<Vec<DataType>>;
type Getter = Box<Fn(&DataType, bool) -> Result<Datas, ()> + Send>;

pub struct Backend {
    getters: HashMap<String, Getter>,
    mutators: HashMap<String, Mutator>,
    recipe: Option<Recipe>,
    pub soup: Arc<Mutex<Blender>>,
}

impl Backend {
    pub fn new(soup: Blender, recipe: Recipe) -> Backend {
        let soup = Arc::new(Mutex::new(soup));
        let soup2 = soup.clone();
        thread::spawn(||web::run(soup2));

        Backend {
            getters: HashMap::default(),
            mutators: HashMap::default(),
            recipe: Some(recipe),
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
        let mtr = self.mutators
            .entry(String::from(kind))
            .or_insert(self.soup
                       .lock()
                       .unwrap()
                       .get_mutator(self.recipe.as_ref().unwrap().node_addr_for(kind)?));

        mtr.put(data);
        Ok(())
    }

    pub fn get<I>(&mut self, kind: &str, key: I) -> Result<Datas, String>
        where I: Into<DataType>
    {
        let get_fn = self.getters
            .entry(String::from(kind))
            .or_insert(self.soup
                           .lock()
                           .unwrap()
                           .get_getter(self.recipe.as_ref().unwrap().node_addr_for(kind)?)
                           .unwrap());

        match get_fn(&key.into(), true) {
            Ok(records) => Ok(records),
            Err(_) => Err(format!("GET for {} failed!", kind)),
        }
    }
}
