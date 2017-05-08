extern crate distributary;
extern crate nom_sql;
extern crate rustyline;
#[macro_use]
extern crate slog;
extern crate slog_term;

use distributary::{Blender, Recipe};
use nom_sql::SqlQuery;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use slog::{Level, LevelFilter};
use slog::DrainExt;

fn migrate(current_recipe: Recipe, g: &mut Blender, line: &str) -> Result<Recipe, String> {
    // try to add query to recipe
    match current_recipe.extend(line) {
        Ok(mut new_recipe) => {
            let mut mig = g.start_migration();
            match new_recipe.activate(&mut mig, false) {
                Ok(_) => {
                    mig.commit();
                    Ok(new_recipe)
                }
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

fn main() {
    // `()` means no completer is required
    let mut rl = Editor::<()>::new();

    println!("\nWelcome to Pan, your interactive Soup shell!\n");

    if let Err(_) = rl.load_history(".pan_history") {
        println!("No previous history.");
    }

    let mut g = distributary::Blender::new();
    let log = slog::Logger::root(LevelFilter::new(slog_term::streamer().build().fuse(),
                                                  Level::Info),
                                 None);
    g.log_with(log.clone());

    let mut current_recipe = distributary::Recipe::blank(None);

    loop {
        let readline = rl.readline("Pan> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line);

                if line.is_empty() {
                    continue;
                }

                match nom_sql::parse_query(&line) {
                    Ok(q) => {
                        match q {
                            SqlQuery::Insert(_) => {
                                // if this is an INSERT query, we want to execute it using an appropriate mutator
                                error!(log, "INSERT queries unsupported");
                            }
                            SqlQuery::CreateTable(_) |
                            SqlQuery::Select(_) => {
                                let prev_recipe = current_recipe.clone();
                                current_recipe = match migrate(current_recipe, &mut g, &line) {
                                    Ok(new_recipe) => {
                                        println!("\n");
                                        new_recipe
                                    }
                                    Err(e) => {
                                        error!(log, "{}", e);
                                        prev_recipe
                                    }
                                }
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
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(".pan_history").unwrap();
}
