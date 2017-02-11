extern crate distributary;
extern crate rustyline;
extern crate slog;
extern crate slog_term;

use rustyline::error::ReadlineError;
use rustyline::Editor;

use slog::{Level, LevelFilter};
use slog::DrainExt;

fn main() {
    // `()` means no completer is required
    let mut rl = Editor::<()>::new();

    println!("\nWelcome to Pan, your interactive Soup migration shell!\n");

    if let Err(_) = rl.load_history(".pan_history") {
        println!("No previous history.");
    }

    let mut g = distributary::Blender::new();
    g.log_with(slog::Logger::root(LevelFilter::new(slog_term::streamer().build().fuse(),
                                                   Level::Info),
                                  None));

    let mut current_recipe = distributary::Recipe::blank();

    loop {
        let readline = rl.readline("Pan> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line);

                let prev_recipe = current_recipe.clone();

                // try to add query to recipe
                current_recipe = match current_recipe.extend(&line) {
                    Ok(mut new_recipe) => {
                        let mut mig = g.start_migration();
                        match new_recipe.activate(&mut mig) {
                            Ok(_) => {
                                mig.commit();
                                println!("\n");
                                new_recipe
                            }
                            Err(e) => {
                                println!("{}", e);
                                prev_recipe
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}", e);
                        prev_recipe
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
