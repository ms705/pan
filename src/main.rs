extern crate distributary;
extern crate nom_sql;
extern crate rustyline;
#[macro_use]
extern crate slog;

mod backend;

use backend::Backend;
use nom_sql::SqlQuery;
use rustyline::error::ReadlineError;
use rustyline::Editor;

fn main() {
    // `()` means no completer is required
    let mut rl = Editor::<()>::new();

    println!("\nWelcome to Pan, your interactive Soup shell!\n");

    if let Err(_) = rl.load_history(".pan_history") {
        println!("No previous history.");
    }

    let mut g = distributary::Blender::new();
    let log = distributary::logger_pls();
    g.log_with(log.clone());

    let mut backend = Backend::new(g, distributary::Recipe::blank(None));

    let do_migrate = |backend: &mut Backend, line: &str| match backend.migrate(line) {
        Ok(_) => {
            println!("\n");
        }
        Err(e) => {
            error!(log, "{}", e);
        }
    };

    loop {
        let readline = rl.readline("Pan> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line);

                if line.is_empty() {
                    continue;
                }

                // special, Soup-only SHOW GRAPH query
                if line.trim().to_lowercase() == "show graph;" {
                    println!("\n{}\n", backend.soup);
                    continue;
                }

                match nom_sql::parse_query(&line) {
                    Ok(q) => {
                        match q {
                            SqlQuery::Insert(iq) => {
                                // if this is an INSERT query, we want to execute it using
                                // the appropriate mutator
                                let (_, values): (Vec<_>, Vec<_>) = iq.fields.into_iter().unzip();
                                match backend.put(&iq.table.name,
                                                  values
                                                      .into_iter()
                                                      .map(|v| v.into())
                                                      .collect::<Vec<_>>()
                                                      .as_slice()) {
                                    Ok(_) => info!(log, "Inserted 1 record.\n"),
                                    Err(e) => {
                                        error!(log, "{}", e);
                                    }
                                }
                            }
                            SqlQuery::CreateTable(_) => do_migrate(&mut backend, &line),
                            SqlQuery::Select(_) => {
                                do_migrate(&mut backend, &line);
                                // if not a parameterized query, execute
                            }
                        }
                    }
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
                error!(log, "{:?}", err);
                break;
            }
        }
    }
    rl.save_history(".pan_history").unwrap();
}
