extern crate clap;
extern crate distributary;
extern crate nom_sql;
extern crate rustyline;
#[macro_use]
extern crate slog;
extern crate slog_term;

mod backend;

use backend::Backend;
use distributary::DataType;
use nom_sql::{ConditionBase, ConditionExpression, Literal, SqlQuery};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::str::FromStr;

fn make_logger(level: slog::Level) -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).filter_level(level).fuse(), o!())
}

fn extract_query_parameters(wc: ConditionExpression) -> Vec<String> {
    let mut params = vec![];
    match wc {
        ConditionExpression::LogicalOp(ct) => {
            params.extend(extract_query_parameters(*ct.left));
            params.extend(extract_query_parameters(*ct.right));
        }
        ConditionExpression::ComparisonOp(ct) => {
            match *ct.right {
                ConditionExpression::Base(ConditionBase::Placeholder) => {
                    match *ct.left {
                        ConditionExpression::Base(ConditionBase::Field(f)) => {
                            params.push(f.name);
                        }
                        _ => (),
                    }
                }
                _ => (),
            }
        }
        _ => panic!(),
    }
    params
}

fn handle_query(backend: &mut Backend, mut line: &str, log: &slog::Logger) -> Result<(), String> {
    let name = line.find(':').map(|i| {
                                      let name = line[..i].trim();
                                      line = &line[i + 1..].trim();
                                      name
                                  });

    if name.is_some() && backend.query_exists(name.as_ref().unwrap()) {
        return Err(format!("Query with name '{}' already exists", name.unwrap()));
    }

    match nom_sql::parse_query(line) {
        Ok(q) => {
            match q {
                SqlQuery::Insert(iq) => {
                    // if this is an INSERT query, we want to execute it using
                    // the appropriate mutator
                    let (_, values): (Vec<_>, Vec<_>) = iq.fields.into_iter().unzip();
                    match backend.put(&iq.table.name,
                                      values
                                          .into_iter()
                                          .map(|v| match v {
                                                   Literal::String(s) => s.into(),
                                                   Literal::Integer(i) => i.into(),
                                                   Literal::Null => DataType::None,
                                                   _ => unimplemented!(),
                                               })
                                          .collect::<Vec<_>>()
                                          .as_slice()) {
                        Ok(_) => {
                            info!(log, "Inserted 1 record into \"{}\".\n", iq.table.name);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                SqlQuery::CreateTable(_) => {
                    // only need to do a migration to install the new table
                    match backend.migrate(line) {
                        Ok(_) => {
                            print!("\n");
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                SqlQuery::Select(sq) => {
                    // first do a migration to add the query (may be a no-op if we can reuse
                    // existing queries)
                    match backend.migrate(line) {
                        Ok(act_res) => {
                            let params = match sq.where_clause {
                                None => vec![],
                                Some(wc) => extract_query_parameters(wc),
                            };

                            assert!(act_res.new_nodes.len() <= 1);

                            for (t, _) in act_res.new_nodes {
                                info!(log, "Added new query {}({}).\n", t, params.join(", "));

                                if let Some(ref name) = name {
                                    backend.add_query(name, &t, params.len());
                                }

                                // // if not a parameterized query, execute
                                // // XXX(malte): also execute if the query already existed and
                                // // wasn't added by the migration!
                                // // XXX(malte): handle parameterized queries
                                // match backend.get(&t, DataType::BigInt(0)) {
                                //     Ok(qres) => {
                                //         let count = qres.len();
                                //         for r in qres {
                                //             println!("{}",
                                //                      r.into_iter()
                                //                          .map(|c| format!("{}", c))
                                //                          .collect::<Vec<_>>()
                                //                          .join(", "));
                                //         }
                                //         println!("\nQuery returned {} rows.\n", count);
                                //     }
                                //     Err(e) => return Err(e),
                                // }
                            }
                        }
                        Err(e) => return Err(e),
                    }

                    Ok(())
                }
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

fn handle_execute(backend: &mut Backend, s: nom_sql::ExecuteStatement) -> Result<(), String> {
    let params: Vec<DataType> = s.values
        .into_iter()
        .map(|l| match l {
                 Literal::Integer(i) => i.into(),
                 Literal::String(s) => s.into(),
                 _ => unimplemented!(),
             })
        .collect();

    match backend.execute_query(&s.table.name, &params) {
        Ok(qres) => {
            let count = qres.len();
            for r in qres {
                println!("{}",
                         r.into_iter()
                             .map(|c| format!("{}", c))
                             .collect::<Vec<_>>()
                             .join(", "));
            }
            println!("\nQuery returned {} rows.\n", count);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn main() {
    use clap::{Arg, App};
    use std::io::Read;
    use std::fs::File;

    let matches = App::new("pan")
        .version("0.0.1")
        .about("Interactive Soup shell.")
        .arg(Arg::with_name("recipe")
                 .short("r")
                 .long("recipe")
                 .takes_value(true)
                 .help("Recipe file to start from."))
        .arg(Arg::with_name("nopartial")
                 .long("no-partial-materialization")
                 .help("Disable partial materialization."))
        .arg(Arg::with_name("noshard")
                 .long("no-sharding")
                 .help("Disable sharding"))
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let start_recipe_file = matches.value_of("recipe");
    let partial = !matches.is_present("nopartial");
    let sharding = !matches.is_present("noshard");
    let verbose = matches.is_present("verbose");

    // `()` means no completer is required
    let mut rl = Editor::<()>::new();

    println!("\nWelcome to Pan, your interactive Soup shell!\n");

    if let Err(_) = rl.load_history(".pan_history") {
        println!("No previous history.");
    }

    let mut g = distributary::Blender::new();
    let log = if verbose {
        make_logger(slog::Level::Info)
    } else {
        make_logger(slog::Level::Error)
    };

    g.log_with(log.clone());

    if !partial {
        g.disable_partial();
    }

    if !sharding {
        g.disable_sharding();
    }

    let mut backend = Backend::new(g, distributary::Recipe::blank(Some(log.clone())));

    match start_recipe_file {
        None => (),
        Some(rf) => {
            let mut f = match File::open(rf) {
                Ok(f) => f,
                Err(e) => {
                    error!(log, "Failed to open initial recipe: {}", e);
                    return;
                }
            };
            let mut s = String::new();
            match f.read_to_string(&mut s) {
                Ok(_) => {
                    match backend.migrate(&s) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(log, "Failed to apply initial recipe: {}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!(log, "Failed to read initial recipe: {}", e);
                    return;
                }
            }
        }
    }

    loop {
        let readline = rl.readline("Pan> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.clone());

                let mut line = String::from_str(line.trim()).unwrap();
                if line.is_empty() {
                    continue;
                }

                if line.chars().rev().next().unwrap() != ';' {
                    line.push(';');
                }

                // special, Soup-only SHOW GRAPH query
                if line.trim().to_lowercase() == "show graph;" {
                    let soup = backend.soup.lock().unwrap();
                    println!("\n{}\n", *soup);
                    continue;
                }

                let execute = nom_sql::execute_statement(line.as_bytes());
                let result = if let nom_sql::IResult::Done(_, e) = execute {
                    handle_execute(&mut backend, e)
                } else {
                    handle_query(&mut backend, &line, &log)
                };

                match result {
                    Ok(_) => (),
                    Err(e) => {
                        error!(log, "{}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                error!(log, "{}", err);
                break;
            }
        }
    }
    rl.save_history(".pan_history").unwrap();
}
