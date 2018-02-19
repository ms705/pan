use std::collections::{BTreeMap, HashMap};

use distributary::{ActivationResult, ControllerHandle, DataType, Mutator, MutatorError, NodeIndex,
                   RemoteGetter, RpcError, ZookeeperAuthority};

type Datas = Vec<Vec<DataType>>;

pub struct Backend {
    inputs: BTreeMap<String, NodeIndex>,
    outputs: BTreeMap<String, NodeIndex>,
    getters: HashMap<String, RemoteGetter>,
    mutators: HashMap<String, Mutator>,
    queries: HashMap<String, (String, usize)>,
    pub soup: ControllerHandle<ZookeeperAuthority>,
}

impl Backend {
    pub fn new(mut soup: ControllerHandle<ZookeeperAuthority>) -> Backend {
        let inputs = soup.inputs();
        let outputs = soup.outputs();

        Backend {
            inputs: inputs,
            outputs: outputs,
            getters: HashMap::default(),
            mutators: HashMap::default(),
            queries: HashMap::default(),
            soup: soup,
        }
    }

    pub fn migrate(&mut self, line: &str) -> Result<ActivationResult, RpcError> {
        // try to add query to recipe
        let ar = self.soup.extend_recipe(line.to_owned())?;
        self.inputs = self.soup.inputs();
        self.outputs = self.soup.outputs();
        Ok(ar)
    }

    pub fn put(&mut self, kind: &str, data: &[DataType]) -> Result<(), String> {
        let mtr = self.mutators.entry(String::from(kind)).or_insert(self.soup
            .get_mutator(*self.inputs
                .get(kind)
                .map_or(Err(format!("No table named '{}'", kind)), |m| Ok(m))?)
            .map_err(|e| e.description().to_owned())?);

        mtr.put(data).map_err(|e| match e {
            MutatorError::WrongColumnCount(expected, got) => format!(
                "Wrong number of columns specified: expected {}, got {}",
                expected, got
            ),
            MutatorError::TransactionFailed => unreachable!(),
        })
    }

    pub fn add_query(&mut self, name: &str, kind: &str, num_params: usize) {
        self.queries.insert(name.into(), (kind.into(), num_params));
    }

    pub fn execute_query(&mut self, name: &str, params: &[DataType]) -> Result<Datas, String> {
        if params.len() != 1 {
            return Err(format!(
                "Only single parameter queries are currently supported"
            ));
        }

        let (ref kind, nparams) = *match self.queries.get(name) {
            Some(k) => k,
            None => return Err(format!("Unrecognized query: \"{}\"", name)),
        };

        if nparams != params.len() {
            return Err(format!(
                "Wrong number of values: expected {}, got {}",
                nparams,
                params.len()
            ));
        }

        let ni = self.outputs
            .get(kind)
            .map_or(Err(format!("No view named '{}'", kind)), |ni| Ok(ni))?;
        let getter = self.getters.entry(kind.clone()).or_insert(self.soup
            .get_getter(*ni)
            .map_or(Err(format!("View named '{}' is lost", kind)), |g| Ok(g))?);

        match getter.lookup(&params[0], true) {
            Ok(records) => Ok(records),
            Err(_) => Err(format!("GET for {} failed!", kind)),
        }
    }

    pub fn query_exists(&self, name: &str) -> bool {
        self.queries.contains_key(name)
    }
}
