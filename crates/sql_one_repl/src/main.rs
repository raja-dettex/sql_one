use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};
use sql_one_parser::parser::Parse;
use sql_one_execution::execution::{ExecResponse, Execution};

const HISTORY_FILE: &str = "./history.txt";

fn main() -> Result<()>{
    let mut rl: Editor<(), _> = Editor::new()?;
    if rl.load_history(HISTORY_FILE).is_err() { 
        println!("no previous history");
    }
    let mut exec = Execution::new();
    loop { 
        let readline = rl.readline(">> ");
        match readline { 
            Ok(line) => { 
                rl.add_history_entry(line.as_str());
                let res = exec.parse_and_run(&line);
                match res { 
                    Ok(response) => { 
                        println!("{}", response );
                        if let ExecResponse::Select(res) = response { 
                            for row in res { 
                                println!("row is {:#?}", row);
                            }
                        }
                    
                    },
                    Err(e) => { 
                        println!("{}", e);
                    }
                }
                //println!("sql query = {:#?}", res)
            }
            Err(ReadlineError::Interrupted) => {

            },
            Err(ReadlineError::Eof) => { 
                break
            },
            Err(err) => {
                 println!("error is {:#?}", err);
                 break
            }
        }
    }
    rl.save_history(HISTORY_FILE)
}
