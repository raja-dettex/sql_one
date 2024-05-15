use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};
use sql_one_parser::parser::Parse;
use sql_one_execution::execution::{ExecResponse, Execution};

const HISTORY_FILE: &str = "./history.txt";

fn main() -> Result<()>{
    // let mut exec = Execution::new();
    // match exec.parse_and_run("create table User (id int, name string);") {
    //     Ok(result) => println!("response is {:#?}", result),
    //     Err(err) => println!("error execution : {:#?}", err),
    // }
    // //println!("tables :{:#?}", exec.tables);
    
    // let table = exec.get_table("User");
    
    // println!("storage : {:#?}", table.storage);
    // println!("after insertion storage : {:#?}", table.storage);
    // match exec.parse_and_run("insert into User values 1,'raja';") {
    //     Ok(result) => println!("response is {:#?}", result),
    //     Err(err) => println!("error execution : {:#?}", err),
    // }
    // match exec.parse_and_run("insert into User values 2,'neha';") {
    //     Ok(result) => println!("response is {:#?}", result),
    //     Err(err) => println!("error execution : {:#?}", err),
    // }
    // match exec.parse_and_run("select name from User;") { 
    //     Ok(result) => println!("response is {:#?}", result),
    //     Err(err) => println!("error execution : {:#?}", err),
    // }
    // Ok(())
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
                        
                        match response {
                            ExecResponse::Select(rows) => {
                                for row in rows { 
                                    println!("row is {:#?}", row);
                                }
                            },
                            ExecResponse::Insert => println!("insert"),
                            ExecResponse::Crete => println!("create"),
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
